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

	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
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

// createPODandVerifyVolumeMount this method creates Pod and verifies VolumeMount
func createPODandVerifyVolumeMountWithoutF(ctx context.Context, client clientset.Interface,
	namespace string, pvclaim *v1.PersistentVolumeClaim, volHandle string) (*v1.Pod, string) {
	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	var exists bool
	var vmUUID string
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))

	annotations := pod.Annotations
	vmUUID, exists = annotations[vmUUIDLabel]
	gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

	framework.Logf("VMUUID : %s", vmUUID)
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
		"Volume is not attached to the node volHandle: %s, vmUUID: %s", volHandle, vmUUID)

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name,
		[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return pod, vmUUID
}
