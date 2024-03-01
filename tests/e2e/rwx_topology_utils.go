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
	"context"
	"fmt"
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
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

// createPod with given claims based on node selector.
func createStandalonePodsForRWXVolume(client clientset.Interface, namespace string, nodeSelector map[string]string,
	pvclaims []*v1.PersistentVolumeClaim, isPrivileged bool, command string, readOnly bool) (*v1.Pod, error) {
	var err error
	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, isPrivileged, command)

	pod.Spec.Containers[0].Image = busyBoxImageOnGcr
	pod.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ReadOnly = readOnly

	pod, err = client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// Get fresh pod info.
	pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}

	// var vmUUID string
	// nodeName := pod.Spec.NodeName
	// vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
	// ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
	// isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	// gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify the volume is accessible and Read/write is possible")
	cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
		"cat /mnt/volume1/Pod.html "}
	output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
	gomega.Expect(strings.Contains(output, "Hello message from Pod")).NotTo(gomega.BeFalse())

	if readOnly {
		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod' > /mnt/volume1/Pod.html"}
		output = e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		// Verify that the write operation fails
		gomega.Expect(strings.Contains(output, "Permission denied")).NotTo(gomega.BeFalse())
	} else {
		// For other pods, perform a successful write operation
		wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
			"echo 'Hello message from test into Pod' > /mnt/volume1/Pod.html"}
		e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
	}

	ginkgo.By("Verify volume metadata for POD, PVC and PV")
	err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle, pvclaim, pv, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return pod, nil
}
