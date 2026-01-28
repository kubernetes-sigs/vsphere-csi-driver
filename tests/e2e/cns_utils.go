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

package e2e

import (
	"context"
	"reflect"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
)

// verifyCnsVolumeMetadata4GCVol verifies CNS volume metadata for a GC volume.
// If gcPvc, gcPv or pod are nil, we skip verification for them altogether and won't check if they are absent in CNS entry.
func verifyCnsVolumeMetadata4GCVol(volumeID string, svcPVCName string, gcPvc *v1.PersistentVolumeClaim,
	gcPv *v1.PersistentVolume, pod *v1.Pod) bool {

	framework.Logf("[START] Verifying CNS metadata for volumeID: %s", volumeID)

	// Query CNS for volume metadata
	cnsQueryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if len(cnsQueryResult.Volumes) == 0 {
		framework.Logf("[ERROR] CNS volume query yielded no results for volume id: %s", volumeID)
		return false
	}

	cnsVolume := cnsQueryResult.Volumes[0]
	framework.Logf("[INFO] CNS volume found: %s", spew.Sdump(cnsVolume))

	// Initialize verification flags
	podEntryFound := false
	verifyPvEntry := gcPv != nil
	verifyPvcEntry := gcPvc != nil
	verifyPodEntry := pod != nil
	gcPVCVerified := !verifyPvcEntry
	gcPVVerified := !verifyPvEntry
	svcPVCVerified := false
	svcPVVerified := false

	// Fetch supervisor PVC and PV
	svcPVC := getPVCFromSupervisorCluster(svcPVCName)
	svcPV := getPvFromSupervisorCluster(svcPVCName)
	framework.Logf("[INFO] Retrieved svcPVC: %v", spew.Sdump(svcPVC))
	framework.Logf("[INFO] Retrieved svcPV: %v", spew.Sdump(svcPV))

	// Iterate over CNS entity metadata
	for _, entity := range cnsVolume.Metadata.EntityMetadata {
		entityMetadata := entity.(*cnstypes.CnsKubernetesEntityMetadata)
		framework.Logf("[DEBUG] Processing entity: Type=%s, Name=%s, Namespace=%s", entityMetadata.EntityType, entityMetadata.EntityName, entityMetadata.Namespace)

		switch entityMetadata.EntityType {
		case string(cnstypes.CnsKubernetesEntityTypePVC):
			var pv *v1.PersistentVolume
			var pvc *v1.PersistentVolumeClaim
			verifySvcPvc := entityMetadata.EntityName == svcPVCName
			verifySvcPv := verifySvcPvc

			if verifySvcPvc {
				pvc = svcPVC
				pv = svcPV
			} else {
				pvc = gcPvc
				pv = gcPv
			}

			if (verifyPvcEntry && pvc != nil) || verifySvcPvc {
				if entityMetadata.EntityName != pvc.Name {
					framework.Logf("[MISMATCH] PVC name mismatch: expected='%s', actual='%s'", pvc.Name, entityMetadata.EntityName)
					break
				}
				if (verifyPvEntry && pv != nil) || verifySvcPv {
					if entityMetadata.ReferredEntity == nil {
						framework.Logf("[MISSING] ReferredEntity missing in PVC metadata for volume id %s", volumeID)
						break
					}
					if entityMetadata.ReferredEntity[0].EntityName != pv.Name {
						framework.Logf("[MISMATCH] PV name mismatch in ReferredEntity: expected='%s', actual='%s'", pv.Name, entityMetadata.ReferredEntity[0].EntityName)
						break
					}
				}
				if !reflect.DeepEqual(getLabelMap(entityMetadata.Labels), pvc.Labels) {
					framework.Logf("[MISMATCH] PVC labels mismatch: expected='%v', actual='%v'", pvc.Labels, entityMetadata.Labels)
					break
				}
				if entityMetadata.Namespace != pvc.Namespace {
					framework.Logf("[MISMATCH] PVC namespace mismatch: expected='%s', actual='%s'", pvc.Namespace, entityMetadata.Namespace)
					break
				}
			}

			if verifySvcPvc {
				svcPVCVerified = true
			} else {
				gcPVCVerified = true
			}

		case string(cnstypes.CnsKubernetesEntityTypePV):
			var pv *v1.PersistentVolume
			verifySvcPv := entityMetadata.EntityName == svcPV.Name

			if verifySvcPv {
				pv = svcPV
			} else {
				pv = gcPv
			}

			if (verifyPvEntry && pv != nil) || verifySvcPv {
				if entityMetadata.EntityName != pv.Name {
					framework.Logf("[MISMATCH] PV name mismatch: expected='%s', actual='%s'", pv.Name, entityMetadata.EntityName)
					break
				}
				if !reflect.DeepEqual(getLabelMap(entityMetadata.Labels), pv.Labels) {
					framework.Logf("[MISMATCH] PV labels mismatch: expected='%v', actual='%v'", pv.Labels, entityMetadata.Labels)
					break
				}
				if !verifySvcPv {
					if entityMetadata.ReferredEntity == nil {
						framework.Logf("[MISSING] ReferredEntity missing in PV metadata for volume id %s", volumeID)
						break
					}
					if entityMetadata.ReferredEntity[0].EntityName != svcPVCName {
						framework.Logf("[MISMATCH] SVC PVC name mismatch in ReferredEntity: expected='%s', actual='%s'", svcPVCName, entityMetadata.ReferredEntity[0].EntityName)
						break
					}
					if entityMetadata.ReferredEntity[0].Namespace != svcPVC.Namespace {
						framework.Logf("[MISMATCH] SVC PVC namespace mismatch in ReferredEntity: expected='%s', actual='%s'", svcPVC.Namespace, entityMetadata.ReferredEntity[0].Namespace)
						break
					}
				}
			}

			if verifySvcPv {
				svcPVVerified = true
			} else {
				gcPVVerified = true
			}

		case string(cnstypes.CnsKubernetesEntityTypePOD):
			if verifyPodEntry && pod != nil {
				podEntryFound = true
				if entityMetadata.EntityName != pod.Name {
					framework.Logf("[MISMATCH] Pod name mismatch: expected='%s', actual='%s'", pod.Name, entityMetadata.EntityName)
					podEntryFound = false
					break
				}
				if entityMetadata.Namespace != pod.Namespace {
					framework.Logf("[MISMATCH] Pod namespace mismatch: expected='%s', actual='%s'", pod.Namespace, entityMetadata.Namespace)
					podEntryFound = false
					break
				}
				if verifyPvcEntry && gcPvc != nil {
					if entityMetadata.ReferredEntity == nil {
						framework.Logf("[MISSING] ReferredEntity missing in Pod metadata for volume id %s", volumeID)
						podEntryFound = false
						break
					}
					if entityMetadata.ReferredEntity[0].EntityName != gcPvc.Name {
						framework.Logf("[MISMATCH] PVC name mismatch in Pod ReferredEntity: expected='%s', actual='%s'", gcPvc.Name, entityMetadata.ReferredEntity[0].EntityName)
						podEntryFound = false
						break
					}
					if entityMetadata.ReferredEntity[0].Namespace != gcPvc.Namespace {
						framework.Logf("[MISMATCH] PVC namespace mismatch in Pod ReferredEntity: expected='%s', actual='%s'", gcPvc.Namespace, entityMetadata.ReferredEntity[0].Namespace)
						podEntryFound = false
						break
					}
				}
			}
		}
	}

	framework.Logf("[RESULT] Verification flags — gcPVVerified: %v, verifyPvEntry: %v, gcPVCVerified: %v, verifyPvcEntry: %v, podEntryFound: %v, verifyPodEntry: %v, svcPVCVerified: %v, svcPVVerified: %v",
		gcPVVerified, verifyPvEntry, gcPVCVerified, verifyPvcEntry, podEntryFound, verifyPodEntry, svcPVCVerified, svcPVVerified)

	finalResult := gcPVVerified && gcPVCVerified && podEntryFound == verifyPodEntry && svcPVCVerified && svcPVVerified
	framework.Logf("[END] CNS metadata verification result for volumeID %s: %v", volumeID, finalResult)
	return finalResult
}

// waitAndVerifyCnsVolumeMetadata4GCVol verifies cns volume metadata for a GC volume with wait
func waitAndVerifyCnsVolumeMetadata4GCVol(ctx context.Context, volHandle string, svcPVCName string,
	pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, pod *v1.Pod) error {
	waitErr := wait.PollUntilContextTimeout(ctx, healthStatusPollInterval, pollTimeoutSixMin, true,
		func(ctx context.Context) (bool, error) {
			matches := verifyCnsVolumeMetadata4GCVol(volHandle, svcPVCName, pvc, pv, pod)
			return matches, nil
		})
	return waitErr
}
