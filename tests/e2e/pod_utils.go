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
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
)

func waitForPodNameLabelRemoval(ctx context.Context, volumeID string, podname string, namespace string) error {
	err := wait.PollUntilContextTimeout(ctx, poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			_, err := e2eVSphere.getLabelsForCNSVolume(volumeID,
				string(cnstypes.CnsKubernetesEntityTypePOD), podname, namespace)
			if err != nil {
				framework.Logf("pod name label is successfully removed")
				return true, err
			}
			framework.Logf("waiting for pod name label to be removed by metadata-syncer for volume: %q", volumeID)
			return false, nil
		})
	// unable to retrieve pod name label from vCenter
	if err != nil {
		return nil
	}
	return fmt.Errorf("pod name label is not removed from cns")
}
