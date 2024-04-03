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
	"errors"
	"fmt"
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	cnstypes "github.com/vmware/govmomi/cns/types"

	"golang.org/x/crypto/ssh"
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
func createStorageClassWithMultiplePVCs(client clientset.Interface, namespace string, pvclaimlabels map[string]string,
	scParameters map[string]string, ds string, allowedTopologies []v1.TopologySelectorLabelRequirement,
	bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode,
	storageClassName string, pvcItr int, skipStorageClassCreation bool,
	skipPvcCreation bool) (*storagev1.StorageClass, []*v1.PersistentVolumeClaim, error) {

	var storageclass *storagev1.StorageClass
	var err error
	var pvclaims []*v1.PersistentVolumeClaim

	if !skipStorageClassCreation {
		// creating storage class if skipStorageClassCreation is set to false
		scName := ""
		if len(storageClassName) > 0 {
			scName = storageClassName
		}
		storageclass, err = createStorageClass(client, scParameters,
			allowedTopologies, "", bindingMode, allowVolumeExpansion, scName)
		if err != nil {
			return storageclass, nil, err
		}
	}

	if !skipPvcCreation {
		// creating pvc if skipPvcCreation is set to false
		for i := 0; i < pvcItr; i++ {
			pvclaim, err := createPVC(client, namespace, pvclaimlabels, ds, storageclass, accessMode)
			pvclaims = append(pvclaims, pvclaim)
			if err != nil {
				return storageclass, pvclaims, err
			}
		}
	}
	return storageclass, pvclaims, nil
}

func checkVolumeStateAndPerformCnsVerification(client clientset.Interface, pvclaims []*v1.PersistentVolumeClaim,
	storagePolicyName string, datastoreUrl string) ([]*v1.PersistentVolume, error) {

	var queryResult *cnstypes.CnsQueryResult
	var pvs []*v1.PersistentVolume

	for i := 0; i < len(pvclaims); i++ {
		pv, err := fpv.WaitForPVClaimBoundPhase(client, []*v1.PersistentVolumeClaim{pvclaims[i]},
			framework.ClaimProvisionTimeout)
		pvs = append(pvs, pv[0])
		volHandle := pv[0].Spec.CSI.VolumeHandle
		if err != nil {
			return pvs, err
		}

		// cns side verification
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		if multivc {
			queryResult, err = multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVc(volHandle)
		} else {
			queryResult, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
		}

		if len(queryResult.Volumes) == 0 || err != nil {
			return pvs, fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}

		// if it is an rwx volume
		if rwxAccessMode {
			ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
				queryResult.Volumes[0].Name,
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
				queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
				queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
			)

			framework.Logf("Verifying disk size specified in PVC is honored")
			if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
				return pvs, fmt.Errorf("wrong disk size provisioned")
			}

			framework.Logf("Verify if VolumeID is created on the VSAN datastores")
			if !strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:") {
				return pvs, fmt.Errorf("volume is not provisioned on vSan datastore")
			}
		}

		framework.Logf("Verifying volume type specified in PVC is honored")
		if queryResult.Volumes[0].VolumeType != testVolumeType {
			return pvs, fmt.Errorf("volume type is not %q", testVolumeType)
		}

		framework.Logf("Verify the volume is provisioned using specified storage policy")
		if storagePolicyName != "" {
			storagePolicyID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			if storagePolicyID != queryResult.Volumes[0].StoragePolicyId {
				return pvs, fmt.Errorf("storage policy verification failed. Actual storage policy: %q does not match "+
					"with the Expected storage policy: %q", queryResult.Volumes[0].StoragePolicyId, storagePolicyID)
			}
		}

		framework.Logf("Verify the volume is provisioned on the datastore url specified in the Storage Class")
		if datastoreUrl != "" {
			if queryResult.Volumes[0].DatastoreUrl != datastoreUrl {
				return pvs, fmt.Errorf("volume is not provisioned on wrong datastore")
			}
		}
	}
	return pvs, nil
}

func createStandalonePodsForRWXVolume(client clientset.Interface, ctx context.Context, namespace string, nodeSelector map[string]string,
	pvclaim *v1.PersistentVolumeClaim, isPrivileged bool, command string, no_pods_to_deploy int) ([]*v1.Pod, error) {
	var podList []*v1.Pod

	for i := 0; i <= no_pods_to_deploy; i++ {
		var pod *v1.Pod
		var err error

		// here we are making sure that Pod3 should not get created with READ/WRITE permissions
		if i != 2 {
			pod, err = createPod(client, namespace, nodeSelector, []*v1.PersistentVolumeClaim{pvclaim}, false, execRWXCommandPod)
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

		// // fetch volume metadata details
		// pv := getPvFromClaim(client, pvclaim.Namespace, pvclaim.Name)

		// // Verify volume metadata for POD, PVC, and PV
		// err = waitAndVerifyCnsVolumeMetadata(pv.Spec.CSI.VolumeHandle, pvclaim, pv, pod)
		// if err != nil {
		// 	return nil, fmt.Errorf("error verifying volume metadata: %v", err)
		// }

		podList = append(podList, pod)
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
	pods *v1.PodList, pv *v1.PersistentVolume) error {
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
			}
		}
	}

	return nil
}

func createVerifyAndScaleDeploymentPods(ctx context.Context, client clientset.Interface, namespace string, replica int32,
	scaleDeploymentPod bool, labelsMap map[string]string, pvclaim *v1.PersistentVolumeClaim) (*appsv1.Deployment, *v1.PodList, error) {

	var deployment *appsv1.Deployment
	var pods *v1.PodList
	var err error

	// create deployment Pod
	deployment, err = createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace, []*v1.PersistentVolumeClaim{pvclaim},
		execRWXCommandPod, false, nginxImage)
	if err != nil {
		return deployment, nil, fmt.Errorf("failed to create deployment: %w", err)
	}

	// checking replica pod running status
	framework.Logf("Wait for deployment pods to be up and running")
	pods, err = fdep.GetPodsForDeployment(client, deployment)
	if err != nil {
		return deployment, pods, fmt.Errorf("failed to get pods for deployment: %w", err)
	}
	pod := pods.Items[0]
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return deployment, pods, fmt.Errorf("failed to wait for pod running: %w", err)
	}

	// get deployment pod replicas
	if scaleDeploymentPod {
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		if err != nil {
			return deployment, pods, fmt.Errorf("failed to get deployment: %w", err)
		}

		pods, err = fdep.GetPodsForDeployment(client, deployment)
		if err != nil {
			return deployment, pods, fmt.Errorf("failed to get pods for deployment: %w", err)
		}
		pod = pods.Items[0]
		rep := deployment.Spec.Replicas
		*rep = replica
		deployment.Spec.Replicas = rep
		_, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			return deployment, pods, fmt.Errorf("failed to update deployment: %w", err)
		}

		pods, err = fdep.GetPodsForDeployment(client, deployment)
		if err != nil {
			return deployment, pods, fmt.Errorf("failed to get pods for deployment: %w", err)
		}
		pod = pods.Items[0]
		rep = deployment.Spec.Replicas
		if *rep < replica {
			// this check is when we scale down deployment pods
			err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
			if err != nil {
				return deployment, pods, fmt.Errorf("failed to wait for pod not found: %w", err)
			}
		} else {
			// this check is when we scale up deployment pods, all new replica pod should come to running states
			ginkgo.By("Wait for deployment pods to be up and running")
			err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
			if err != nil {
				return deployment, pods, fmt.Errorf("failed to wait for pod running: %w", err)
			}
		}
	}
	return deployment, pods, nil
}

func fetchDatastoreListMap(ctx context.Context, client clientset.Interface) ([]string, []string, []string, []string, error) {

	var datastoreUrlsRack1, datastoreUrlsRack2, datastoreUrlsRack3, datastoreUrls []string
	nimbusGeneratedK8sVmPwd := GetAndExpectStringEnvVar(nimbusK8sVmPwd)

	sshClientConfig := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password(nimbusGeneratedK8sVmPwd),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	allMasterIps := getK8sMasterIPs(ctx, client)
	masterIp := allMasterIps[0]

	// fetching datacenter details
	dataCenters, err := e2eVSphere.getAllDatacenters(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// fetching cluster details
	clusters, err := getTopologyLevel5ClusterGroupNames(masterIp, sshClientConfig, dataCenters)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// fetching list of datastores available in different racks
	rack1DatastoreListMap, err := getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[0])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rack2DatastoreListMap, err := getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[1])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rack3DatastoreListMap, err := getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[2])
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create datastore map for each cluster
	for _, value := range rack1DatastoreListMap {
		datastoreUrlsRack1 = append(datastoreUrlsRack1, value)
	}

	for _, value := range rack2DatastoreListMap {
		datastoreUrlsRack2 = append(datastoreUrlsRack2, value)
	}

	for _, value := range rack3DatastoreListMap {
		datastoreUrlsRack3 = append(datastoreUrlsRack3, value)
	}

	datastoreUrls = append(datastoreUrls, datastoreUrlsRack1...)
	datastoreUrls = append(datastoreUrls, datastoreUrlsRack2...)
	datastoreUrls = append(datastoreUrls, datastoreUrlsRack3...)

	return datastoreUrlsRack1, datastoreUrlsRack2, datastoreUrlsRack3, datastoreUrls, nil
}

func verifyStandalonePodAffinity(ctx context.Context, client clientset.Interface, pod *v1.Pod, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement) error {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	for _, volumespec := range pod.Spec.Volumes {
		if volumespec.PersistentVolumeClaim != nil {
			// get pv details
			pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
			if pv == nil {
				return fmt.Errorf("failed to get PV for claim: %s", volumespec.PersistentVolumeClaim.ClaimName)
			}

			// fetch node details
			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			if err != nil {
				return fmt.Errorf("error getting ready and schedulable nodes: %v", err)
			}
			if !(len(nodeList.Items) > 0) {
				return errors.New("no ready and schedulable nodes found")
			}

			// verify pod is running on appropriate nodes
			ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
			res, err := verifyPodLocationLevel5(pod, nodeList, allowedTopologiesMap)
			if err != nil {
				return fmt.Errorf("error verifying pod location: %v", err)
			}
			if !res {
				return fmt.Errorf("pod %v is not running on appropriate node as specified in allowed "+
					"topologies of Storage Class", pod.Name)
			}
		}
	}
	return nil
}
