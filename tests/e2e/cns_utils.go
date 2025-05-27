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

// verifyCnsVolumeMetadata4GCVol verifies cns volume metadata for a GC volume
// if gcPvc, gcPv or pod are nil we skip verification for them altogether and wont check if they are absent in CNS entry
func verifyCnsVolumeMetadata4GCVol(volumeID string, svcPVCName string, gcPvc *v1.PersistentVolumeClaim,
	gcPv *v1.PersistentVolume, pod *v1.Pod) bool {

	cnsQueryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if len(cnsQueryResult.Volumes) == 0 {
		framework.Logf("CNS volume query yielded no results for volume id: %s", volumeID)
		return false
	}
	cnsVolume := cnsQueryResult.Volumes[0]
	podEntryFound := false
	verifyPvEntry := false
	verifyPvcEntry := false
	verifyPodEntry := false
	gcPVCVerified := false
	gcPVVerified := false
	svcPVCVerified := false
	svcPVVerified := false
	svcPVC := getPVCFromSupervisorCluster(svcPVCName)
	svcPV := getPvFromSupervisorCluster(svcPVCName)
	if gcPvc != nil {
		verifyPvcEntry = true
	} else {
		gcPVCVerified = true
	}
	if gcPv != nil {
		verifyPvEntry = true
	} else {
		gcPVVerified = true
	}
	if pod != nil {
		verifyPodEntry = true
	}
	framework.Logf("Found CNS volume with id %v\n"+spew.Sdump(cnsVolume), volumeID)
	gomega.Expect(cnsVolume.Metadata).NotTo(gomega.BeNil())
	for _, entity := range cnsVolume.Metadata.EntityMetadata {
		var pvc *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		entityMetadata := entity.(*cnstypes.CnsKubernetesEntityMetadata)
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePVC) {
			verifySvcPvc := false
			verifySvcPv := false
			if entityMetadata.EntityName == svcPVCName {
				pvc = svcPVC
				pv = svcPV
				verifySvcPvc = true
				verifySvcPv = true
			} else {
				pvc = gcPvc
				pv = gcPv
			}
			if verifyPvcEntry || verifySvcPvc {
				if entityMetadata.EntityName != pvc.Name {
					framework.Logf("PVC name '%v' does not match PVC name in metadata '%v', for volume id %v",
						pvc.Name, entityMetadata.EntityName, volumeID)
					break
				}
				if verifyPvEntry || verifySvcPv {
					if entityMetadata.ReferredEntity == nil {
						framework.Logf("Missing ReferredEntity in PVC entry for volume id %v", volumeID)
						break
					}
					if entityMetadata.ReferredEntity[0].EntityName != pv.Name {
						framework.Logf("PV name '%v' in referred entity does not match PV name '%v', "+
							"in PVC metadata for volume id %v", entityMetadata.ReferredEntity[0].EntityName,
							pv.Name, volumeID)
						break
					}
				}
				if pvc.Labels == nil {
					if entityMetadata.Labels != nil {
						framework.Logf("PVC labels '%v' does not match PVC labels in metadata '%v', for volume id %v",
							pvc.Labels, entityMetadata.Labels, volumeID)
						break
					}
				} else {
					labels := getLabelMap(entityMetadata.Labels)
					if !(reflect.DeepEqual(labels, pvc.Labels)) {
						framework.Logf(
							"Labels on pvc '%v' are not matching with labels in metadata '%v' for volume id %v",
							pvc.Labels, entityMetadata.Labels, volumeID)
						break
					}
				}
				if entityMetadata.Namespace != pvc.Namespace {
					framework.Logf(
						"PVC namespace '%v' does not match PVC namespace in pvc metadata '%v', for volume id %v",
						pvc.Namespace, entityMetadata.Namespace, volumeID)
					break
				}
			}

			if verifySvcPvc {
				svcPVCVerified = true
			} else {
				gcPVCVerified = true
			}
			continue
		}
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePV) {
			verifySvcPv := false
			if entityMetadata.EntityName == svcPV.Name {
				pvc = svcPVC
				pv = svcPV
				verifySvcPv = true
			} else {
				pvc = gcPvc
				pv = gcPv
			}
			if verifyPvEntry || verifySvcPv {
				if entityMetadata.EntityName != pv.Name {
					framework.Logf("PV name '%v' does not match PV name in metadata '%v', for volume id %v",
						pv.Name, entityMetadata.EntityName, volumeID)
					break
				}
				if pv.Labels == nil {
					if entityMetadata.Labels != nil {
						framework.Logf("PV labels '%v' does not match PV labels in metadata '%v', for volume id %v",
							pv.Labels, entityMetadata.Labels, volumeID)
						break
					}
				} else {
					labels := getLabelMap(entityMetadata.Labels)
					if !(reflect.DeepEqual(labels, pv.Labels)) {
						framework.Logf(
							"Labels on pv '%v' are not matching with labels in pv metadata '%v' for volume id %v",
							pv.Labels, entityMetadata.Labels, volumeID)
						break
					}
				}
				if !verifySvcPv {
					if entityMetadata.ReferredEntity == nil {
						framework.Logf("Missing ReferredEntity in SVC PV entry for volume id %v", volumeID)
						break
					}
					if entityMetadata.ReferredEntity[0].EntityName != svcPVCName {
						framework.Logf("SVC PVC name '%v' in referred entity does not match SVC PVC name '%v', "+
							"in SVC PV metadata for volume id %v", entityMetadata.ReferredEntity[0].EntityName,
							svcPVCName, volumeID)
						break
					}
					if entityMetadata.ReferredEntity[0].Namespace != svcPVC.Namespace {
						framework.Logf("SVC PVC namespace '%v' does not match SVC PVC namespace in SVC PV referred "+
							"entity metadata '%v', for volume id %v",
							pvc.Namespace, entityMetadata.ReferredEntity[0].Namespace, volumeID)
						break
					}
				}
			}
			if verifySvcPv {
				svcPVVerified = true
			} else {
				gcPVVerified = true
			}
			continue
		}
		if entityMetadata.EntityType == string(cnstypes.CnsKubernetesEntityTypePOD) {
			pvc = gcPvc
			if verifyPodEntry {
				podEntryFound = true
				if entityMetadata.EntityName != pod.Name {
					framework.Logf("POD name '%v' does not match Pod name in metadata '%v', for volume id %v",
						pod.Name, entityMetadata.EntityName, volumeID)
					podEntryFound = false
					break
				}
				if verifyPvcEntry {
					if entityMetadata.ReferredEntity == nil {
						framework.Logf("Missing ReferredEntity in pod entry for volume id %v", volumeID)
						podEntryFound = false
						break
					}
					if entityMetadata.ReferredEntity[0].EntityName != pvc.Name {
						framework.Logf("PVC name '%v' in referred entity does not match PVC name '%v', "+
							"in PVC metadata for volume id %v", entityMetadata.ReferredEntity[0].EntityName,
							pvc.Name, volumeID)
						podEntryFound = false
						break
					}
					if entityMetadata.ReferredEntity[0].Namespace != pvc.Namespace {
						framework.Logf("PVC namespace '%v' does not match PVC namespace in Pod metadata "+
							"referered entity, '%v', for volume id %v",
							pvc.Namespace, entityMetadata.ReferredEntity[0].Namespace, volumeID)
						podEntryFound = false
						break
					}
				}
				if entityMetadata.Namespace != pod.Namespace {
					framework.Logf(
						"Pod namespace '%v' does not match pod namespace in pvc metadata '%v', for volume id %v",
						pod.Namespace, entityMetadata.Namespace, volumeID)
					podEntryFound = false
					break
				}
			}
		}
	}
	framework.Logf("gcPVVerified: %v, verifyPvEntry: %v, gcPVCVerified: %v, verifyPvcEntry: %v, "+
		"podEntryFound: %v, verifyPodEntry: %v, svcPVCVerified: %v, svcPVVerified: %v ", gcPVVerified, verifyPvEntry,
		gcPVCVerified, verifyPvcEntry, podEntryFound, verifyPodEntry, svcPVCVerified, svcPVVerified)
	return gcPVVerified && gcPVCVerified && podEntryFound == verifyPodEntry && svcPVCVerified && svcPVVerified
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
