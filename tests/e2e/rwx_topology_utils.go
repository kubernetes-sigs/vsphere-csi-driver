/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

/*
 */
func createRwxPVCwithStorageClass(client clientset.Interface, namespace string, pvclaimlabels map[string]string, scParameters map[string]string, ds string, allowedTopologies []v1.TopologySelectorLabelRequirement,
	bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode) (*storagev1.StorageClass, *v1.PersistentVolumeClaim, []*v1.PersistentVolume, error) {

	storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, pvclaimlabels, scParameters, "", allowedTopologies, bindingMode, false, accessMode)
	if err != nil {
		return nil, nil, nil, err
	}

	// Waiting for PVC to be bound

	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	pv, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
	if err != nil {
		return nil, nil, nil, err
	}

	volHandle := pv[0].Spec.CSI.VolumeHandle

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	if err != nil {
		return nil, nil, nil, err
	}

	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*types.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus))

	// Verifying disk size specified in PVC is honored
	if queryResult.Volumes[0].BackingObjectDetails.(*types.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
		return nil, nil, nil, fmt.Errorf("wrong disk size provisioned")
	}

	// Verifying volume type specified in PVC is honored
	if queryResult.Volumes[0].VolumeType != testVolumeType {
		return nil, nil, nil, fmt.Errorf("volume type is not %q", testVolumeType)
	}

	// Verify if VolumeID is created on the VSAN datastores
	if !strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:") {
		return nil, nil, nil, fmt.Errorf("volume is not provisioned on vSan datastore")
	}

	// If everything is successful, return the results
	return storageclass, pvclaim, pv, nil
}
