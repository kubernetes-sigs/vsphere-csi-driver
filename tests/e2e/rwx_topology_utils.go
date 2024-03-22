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
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

const (
	filePath1 = "/mnt/volume1/file1.txt"
	filePath2 = "/mnt/volume1/file2.txt"
)

/*
 */
func createRwxPvcWithStorageClass(client clientset.Interface, namespace string, pvclaimlabels map[string]string, scParameters map[string]string, ds string, allowedTopologies []v1.TopologySelectorLabelRequirement,
	bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode, isMultiVc bool) (*storagev1.StorageClass, *v1.PersistentVolumeClaim, *v1.PersistentVolume, error) {

	var queryResult *types.CnsQueryResult
	var pv *v1.PersistentVolume
	storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, pvclaimlabels, scParameters, "", allowedTopologies, bindingMode, false, accessMode)
	if err != nil {
		return nil, nil, nil, err
	}

	framework.Logf("Waiting for PVC to be in Bound state")
	if bindingMode == "" {
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		_, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		if err != nil {
			return nil, nil, nil, err
		}

		pv = getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
		volHandle := pv.Spec.CSI.VolumeHandle

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		if isMultiVc {
			queryResult, err = multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVc(volHandle)
		} else {
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		}
		if err != nil {
			return nil, nil, nil, err
		}

		ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s",
			queryResult.Volumes[0].Name,
			queryResult.Volumes[0].BackingObjectDetails.(*types.CnsVsanFileShareBackingDetails).CapacityInMb,
			queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus))

		framework.Logf("Verifying disk size specified in PVC is honored")
		if queryResult.Volumes[0].BackingObjectDetails.(*types.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
			return nil, nil, nil, fmt.Errorf("wrong disk size provisioned")
		}

		framework.Logf("Verifying volume type specified in PVC is honored")
		if queryResult.Volumes[0].VolumeType != testVolumeType {
			return nil, nil, nil, fmt.Errorf("volume type is not %q", testVolumeType)
		}

		framework.Logf("Verify if VolumeID is created on the VSAN datastores")
		if !strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:") {
			return nil, nil, nil, fmt.Errorf("volume is not provisioned on vSan datastore")
		}
	}

	// If everything is successful, return the results
	return storageclass, pvclaim, pv, nil
}

func createStandalonePodsForRWXVolume(client clientset.Interface, ctx context.Context, namespace string, nodeSelector map[string]string,
	pvclaim *v1.PersistentVolumeClaim, isPrivileged bool, command string, no_pods_to_deploy int) ([]*v1.Pod, error) {
	var podList []*v1.Pod

	// fetch volume metadata details
	pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)
	volHandle := pv.Spec.CSI.VolumeHandle

	for i := 0; i <= no_pods_to_deploy; i++ {
		var pod *v1.Pod
		var err error

		// here we are making sure that Pod3 should not get created with READ/WRITE permissions
		if i != 2 {
			pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod)
			if err != nil {
				return nil, fmt.Errorf("failed to create pod: %v", err)
			}

			framework.Logf("Verify the volume is accessible and Read/write is possible")
			cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod.html "}
			output, err := e2ekubectl.RunKubectl(namespace, cmd...)
			if err != nil {
				return nil, fmt.Errorf("error executing command on pod %s: %v", pod.Name, err)
			}
			if !strings.Contains(output, "Hello message from Pod") {
				return nil, fmt.Errorf("expected message not found in pod output")
			}

			framework.Logf("Veirfy read/write permission for a pod")
			wrtiecmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod' > /mnt/volume1/Pod.html"}
			e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
		}

		// here we are creating Pod3 with read-only permissions
		if i == 2 {
			pod = fpod.MakePod(namespace, nodeSelector, pvclaims, isPrivileged, command)
			pod.Spec.Containers[0].Image = busyBoxImageOnGcr
			createdPod, err := client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
			if err != nil {
				return nil, fmt.Errorf("error creating pod: %v", err)
			}
			pod = createdPod

			writeCmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"echo 'Hello message from test into Pod' > /mnt/volume1/Pod.html"}
			output, err := e2ekubectl.RunKubectl(namespace, writeCmd...)
			if err != nil {
				framework.Logf("error executing write command on pod %s due to readonly permissions: %v", pod.Name, err)
			}

			framework.Logf("Verify that the write operation fails for readOnly Pod")
			if !strings.Contains(output, "container not found") {
				framework.Logf("error executing write command on pod %s due to readonly permissions: %v", pod.Name, err)
			}

		}

		framework.Logf("Now creating and accessing files from different pods having read/write permissions")
		// i=0 means creating a file for pod0
		if i == 0 {
			ginkgo.By("Create file1.txt on Pod1")
			err := e2eoutput.CreateEmptyFileOnPod(namespace, pod.Name, filePath1)
			if err != nil {
				return nil, fmt.Errorf("error creating file1.txt on Pod: %v", err)
			}

			//Write data on file1.txt on Pod1
			data := "This file file1 is written by Pod1"
			ginkgo.By("Write on file1.txt from Pod1")
			writeDataOnFileFromPod(namespace, pod.Name, filePath1, data)
		}
		// i=1 means creating a file for pod1
		if i == 1 {
			//Read file1.txt created from Pod2
			ginkgo.By("Read file1.txt from Pod2 created by Pod1")
			output := readFileFromPod(namespace, pod.Name, filePath1)
			ginkgo.By(fmt.Sprintf("File contents from file1.txt are: %s", output))
			data := "This file file1 is written by Pod1"
			data = data + " \n Hello from pod2"
			ginkgo.By(fmt.Sprintf("File contents from file1.txt are: %s", data))

			//Create a file file2.txt from Pod2
			err := e2eoutput.CreateEmptyFileOnPod(namespace, pod.Name, filePath2)
			if err != nil {
				return nil, fmt.Errorf("error creating file2.txt on Pod: %v", err)
			}

			//Write to the file
			ginkgo.By("Write on file2.txt from Pod2")
			data = "This file file2 is written by Pod2"
			writeDataOnFileFromPod(namespace, pod.Name, filePath2, data)
		}

		// framework.Logf("Verify pod %d is attached to the node ", i)
		// var vmUUID string
		// nodeName := pod.Spec.NodeName
		// vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		// ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, nodeName))
		// isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		podList = append(podList, pod)

		//  perform cns metadata verification for any one pod
		// Verify volume metadata for POD, PVC, and PV
		err = waitAndVerifyCnsVolumeMetadata(volHandle, pvclaim, pv, pod)
		if err != nil {
			return nil, fmt.Errorf("error verifying volume metadata: %v", err)
		}
	}

	return podList, nil
}

func getNodeSelectorMapForDeploymentPods(allowedTopologies []v1.TopologySelectorLabelRequirement) map[string]string {
	nodeSelectorMap := make(map[string]string)
	rackTopology := allowedTopologies[len(allowedTopologies)-1]

	for i := 0; i < len(allowedTopologies)-1; i++ {
		topologySelector := allowedTopologies[i]
		nodeSelectorMap[topologySelector.Key] = strings.Join(topologySelector.Values, ",")
	}

	for i := 0; i < len(rackTopology.Values); i++ {
		nodeSelectorMap[rackTopology.Key] = rackTopology.Values[i]
	}

	return nodeSelectorMap
}

func verifyDeploymentPodNodeAffinity(ctx context.Context, client clientset.Interface,
	namespace string, allowedTopologies []v1.TopologySelectorLabelRequirement,
	isMultiVcSetup bool, pods *v1.PodList, pv *v1.PersistentVolume) error {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	for _, pod := range pods.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {

				// fetch node details
				nodeList, err := fnodes.GetReadySchedulableNodes(client)
				if err != nil {
					return err
				}
				if len(nodeList.Items) <= 0 {
					return fmt.Errorf("unable to find ready and schedulable Node")
				}

				// verify pod is running on appropriate nodes
				ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
				res, err := verifyPodLocationLevel5(&pod, nodeList, allowedTopologiesMap)
				if res {
					framework.Logf("Pod %v is running on an appropriate node as specified in the "+
						"allowed topologies of Storage Class", pod.Name)
				}
				if !res {
					return fmt.Errorf("pod %v is not running on an appropriate node as specified "+
						"in the allowed topologies of Storage Class", pod.Name)
				}
				if err != nil {
					return err
				}

				// Verify the attached volume match the one in CNS cache
				if !isMultiVcSetup {
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					if err != nil {
						return err
					}
				} else {
					err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func scaleDeploymentPods(ctx context.Context, client clientset.Interface, deployment *appsv1.Deployment, namespace string, replica int32) (*appsv1.Deployment, error) {

	deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pods, err := fdep.GetPodsForDeployment(client, deployment)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pod := pods.Items[0]
	rep := deployment.Spec.Replicas
	*rep = replica
	deployment.Spec.Replicas = rep
	_, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return deployment, nil
}
