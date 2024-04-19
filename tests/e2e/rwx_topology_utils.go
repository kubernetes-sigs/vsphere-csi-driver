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
	"github.com/vmware/govmomi/object"
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

/*
The createStorageClassWithMultiplePVCs utility function facilitates the creation of a storage class
along with multiple PVCs. The decision to skip or create PVCs or the storage class
is controlled by the user through a boolean parameter.
Additionally, the user can specify the scale of PVC creation using an iterator variable.
This function returns a list containing the PVCs and storage class created,
along with an error message in case of any failures.
*/
func createStorageClassWithMultiplePVCs(client clientset.Interface, namespace string, pvclaimlabels map[string]string,
	scParameters map[string]string, ds string, allowedTopologies []v1.TopologySelectorLabelRequirement,
	bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode,
	storageClassName string, storageClass *storagev1.StorageClass, pvcItr int, skipStorageClassCreation bool,
	skipPvcCreation bool) (*storagev1.StorageClass, []*v1.PersistentVolumeClaim, error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sc *storagev1.StorageClass
	var err error
	var pvclaims []*v1.PersistentVolumeClaim

	if !skipStorageClassCreation {
		// creating storage class if skipStorageClassCreation is set to false
		scName := ""
		if len(storageClassName) > 0 {
			scName = storageClassName
		}
		sc, err = createStorageClass(client, scParameters,
			allowedTopologies, "", bindingMode, allowVolumeExpansion, scName)
		if err != nil {
			return sc, nil, err
		}
	}

	if !skipPvcCreation {
		// we need storage class name in case we are only creating pvcs and skipping sc creation
		if skipStorageClassCreation {
			sc = storageClass
		}
		// creating pvc if skipPvcCreation is set to false
		for i := 0; i < pvcItr; i++ {
			pvclaim, err := createPVC(ctx, client, namespace, pvclaimlabels, ds, sc, accessMode)
			pvclaims = append(pvclaims, pvclaim)
			if err != nil {
				return sc, pvclaims, err
			}
		}
	}
	return sc, pvclaims, nil
}

/*
The checkVolumeStateAndPerformCnsVerification utility function verifies if the PVCs have reached
the Bound state and performs in-depth verification on the CNS Cloud Native Storage side.
It accepts a list of PVCs as input, conducts state and CNS verification for both RWO
and RWX volumes, and returns a list of PVs created successfully or an error message in case of any failures.
*/
func checkVolumeStateAndPerformCnsVerification(ctx context.Context, client clientset.Interface,
	pvclaims []*v1.PersistentVolumeClaim, storagePolicyName string, datastoreUrl string) ([]*v1.PersistentVolume, error) {

	var queryResult *cnstypes.CnsQueryResult
	var pvs []*v1.PersistentVolume
	var storagePolicyID string

	for i := 0; i < len(pvclaims); i++ {
		pv, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvclaims[i]},
			framework.ClaimProvisionTimeout)
		pvs = append(pvs, pv[0])
		volHandle := pv[0].Spec.CSI.VolumeHandle
		if err != nil {
			return pvs, err
		}

		// cns side verification
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		if multivc {
			queryResult, err = multiVCe2eVSphere.queryCNSVolumeWithResultInMultiVC(volHandle)
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
			if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.
				CnsVsanFileShareBackingDetails).CapacityInMb != diskSizeInMb {
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
			if !multivc {
				storagePolicyID = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
				if storagePolicyID != queryResult.Volumes[0].StoragePolicyId {
					return pvs, fmt.Errorf("storage policy verification failed. Actual storage policy: %q does not match "+
						"with the Expected storage policy: %q", queryResult.Volumes[0].StoragePolicyId, storagePolicyID)
				}
			} else {
				storagePolicyID = multiVCe2eVSphere.GetSpbmPolicyIDInMultiVc(storagePolicyName)
				if !strings.Contains(storagePolicyID, queryResult.Volumes[0].StoragePolicyId) {
					return pvs, fmt.Errorf("storage policy verification failed. Actual storage policy: %q does not match "+
						"with the Expected storage policy: %q", queryResult.Volumes[0].StoragePolicyId, storagePolicyID)
				}
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

/*
The createStandalonePodsForRWXVolume utility creates standalone pods for a specified volume.
Users can determine the number of pods to create via a parameter.
The utility ensures that these pods have different read/write permissions.
Additionally, it verifies whether pods with varying permissions can read from or write to the same volume.
Upon completion, the utility returns a list of the created pods or an error message if any failures occur.
*/
func createStandalonePodsForRWXVolume(client clientset.Interface, ctx context.Context, namespace string,
	nodeSelector map[string]string, pvclaim *v1.PersistentVolumeClaim, isPrivileged bool,
	command string, no_pods_to_deploy int) ([]*v1.Pod, error) {
	var podList []*v1.Pod

	for i := 0; i <= no_pods_to_deploy; i++ {
		var pod *v1.Pod
		var err error

		// here we are making sure that Pod3 should not get created with READ/WRITE permissions
		if i != 2 {
			framework.Logf("Creating Pod %d", i)
			pod, err = createPod(ctx, client, namespace, nodeSelector, []*v1.PersistentVolumeClaim{pvclaim},
				false, execRWXCommandPod)
			podList = append(podList, pod)
			if err != nil {
				return podList, fmt.Errorf("failed to create pod: %v", err)
			}

			framework.Logf("Verify the volume is accessible and Read/write is possible")
			cmd := []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c",
				"cat /mnt/volume1/Pod.html "}
			output, err := e2ekubectl.RunKubectl(namespace, cmd...)
			if err != nil {
				return podList, fmt.Errorf("error executing command on pod %s: %v", pod.Name, err)
			}
			if !strings.Contains(output, "Hello message from Pod") {
				return podList, fmt.Errorf("expected message not found in pod output")
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
				return podList, fmt.Errorf("error creating pod: %v", err)
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
				return podList, fmt.Errorf("error creating file1.txt on Pod: %v", err)
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
				return podList, fmt.Errorf("error creating file2.txt on Pod: %v", err)
			}

			//Write to the file
			ginkgo.By("Write on file2.txt from Pod2")
			data = "This file file2 is written by Pod2"
			writeDataOnFileFromPod(namespace, pod.Name, filePath2, data)
		}
	}
	return podList, nil
}

/*
The getNodeSelectorMapForDeploymentPods utility is designed for scenarios where users want to deploy pods
and their replicas in specific availability zones (AZ) of their choice. This utility generates
node selector terms based on the allowed topology specified by the user. It returns a node selector terms
configuration that can be utilized when creating deployment pods.
It returns an error if the provided list of allowed topologies is empty
*/
func getNodeSelectorMapForDeploymentPods(allowedTopologies []v1.TopologySelectorLabelRequirement) (map[string]string,
	error) {
	if len(allowedTopologies) == 0 {
		return nil, errors.New("allowedTopologies cannot be empty")
	}

	nodeSelectorMap := make(map[string]string)
	rackTopology := allowedTopologies[len(allowedTopologies)-1]

	for i := 0; i < len(allowedTopologies)-1; i++ {
		topologySelector := allowedTopologies[i]
		nodeSelectorMap[topologySelector.Key] = strings.Join(topologySelector.Values, ",")
	}

	if len(rackTopology.Values) == 0 {
		return nil, errors.New("no values specified for rack topology")
	}

	for i := 0; i < len(rackTopology.Values); i++ {
		nodeSelectorMap[rackTopology.Key] = rackTopology.Values[i]
	}

	return nodeSelectorMap, nil
}

/*
The verifyDeploymentPodNodeAffinity utility verifies whether pods created during deployment
adhere to the specified node selector term, ensuring they are deployed on the designated
availability zone (AZ) or not.
*/
func verifyDeploymentPodNodeAffinity(ctx context.Context, client clientset.Interface,
	namespace string, allowedTopologies []v1.TopologySelectorLabelRequirement,
	pods *v1.PodList) error {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	for _, pod := range pods.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {

				// fetch node details
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
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

/*
The createVerifyAndScaleDeploymentPods utility manages deployment creation, scaling, and verification.
Users control creation and scaling via boolean parameters, specifying replica counts and the number of deployments.
It returns deployment lists and handles scaling operations, providing scaled deployments and pod lists.
*/

func createVerifyAndScaleDeploymentPods(ctx context.Context, client clientset.Interface,
	namespace string, replica int32, scaleDeploymentPod bool, labelsMap map[string]string,
	pvclaim *v1.PersistentVolumeClaim, nodeSelectorTerms map[string]string, podCmd string,
	podImage string, skipDeploymentCreation bool,
	deploymentToScale *appsv1.Deployment, createDepItr int) ([]*appsv1.Deployment, *v1.PodList, error) {

	var deploymentList []*appsv1.Deployment
	var pods *v1.PodList
	var err error

	// creating deployment when skipDeploymentCreation is set to false
	if !skipDeploymentCreation {
		for i := 0; i < createDepItr; i++ {
			deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nodeSelectorTerms, namespace,
				[]*v1.PersistentVolumeClaim{pvclaim}, podCmd, false, podImage)
			if err != nil {
				return deploymentList, pods, fmt.Errorf("failed to get pods for deployment: %w", err)
			}

			// checking replica pod running status
			framework.Logf("Wait for deployment pods to be up and running")
			pods, err = fdep.GetPodsForDeployment(ctx, client, deployment)
			if err != nil {
				return deploymentList, pods, fmt.Errorf("failed to get pods for deployment: %w", err)
			}
			pod := pods.Items[0]
			err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
			if err != nil {
				return deploymentList, pods, fmt.Errorf("failed to wait for pod running: %w", err)
			}
			deploymentList = append(deploymentList, deployment)
		}
	}

	// get deployment pod replicas
	if scaleDeploymentPod {
		deploymentToScale, err = client.AppsV1().Deployments(namespace).Get(ctx, deploymentToScale.Name, metav1.GetOptions{})
		if err != nil {
			return deploymentList, nil, fmt.Errorf("failed to get pods for deployment: %w", err)
		}
		pods, err = fdep.GetPodsForDeployment(ctx, client, deploymentToScale)
		if err != nil {
			return deploymentList, pods, fmt.Errorf("failed to get pods for deployment: %w", err)
		}
		pod := pods.Items[0]
		rep := deploymentToScale.Spec.Replicas
		*rep = replica
		deploymentToScale.Spec.Replicas = rep
		_, err = client.AppsV1().Deployments(namespace).Update(ctx, deploymentToScale, metav1.UpdateOptions{})
		if err != nil {
			return deploymentList, pods, fmt.Errorf("failed to update deployment: %w", err)
		}

		pods, err = fdep.GetPodsForDeployment(ctx, client, deploymentToScale)
		if err != nil {
			return deploymentList, pods, fmt.Errorf("failed to get pods for deployment: %w", err)
		}
		pod = pods.Items[0]
		rep = deploymentToScale.Spec.Replicas
		if *rep < replica {
			// this check is when we scale down deployment pods
			err = fpod.WaitForPodNotFoundInNamespace(ctx, client, pod.Name, namespace, pollTimeout)
			if err != nil {
				return deploymentList, pods, fmt.Errorf("failed to wait for pod not found: %w", err)
			}
		} else {
			// this check is when we scale up deployment pods, all new replica pod should come to running states
			ginkgo.By("Wait for deployment pods to be up and running")
			err = fpod.WaitForPodRunningInNamespaceSlow(ctx, client, pod.Name, namespace)
			if err != nil {
				return deploymentList, pods, fmt.Errorf("failed to wait for pod running: %w", err)
			}
		}
		deploymentList = append(deploymentList, deploymentToScale)
	}
	return deploymentList, pods, nil
}

/*
The fetchDatastoreListMap utility returns the individual datastore URLs for each VC cluster and
also retruns a combined list of all datastore URLs across all VCs. If any errors occur during this process,
it returns an error appropriately.
*/
func fetchDatastoreListMap(ctx context.Context,
	client clientset.Interface) ([]string, []string, []string, []string, error) {

	var datastoreUrlsRack1, datastoreUrlsRack2, datastoreUrlsRack3, datastoreUrls []string
	nimbusGeneratedK8sVmPwd := GetAndExpectStringEnvVar(nimbusK8sVmPwd)
	var dataCenters []*object.Datacenter
	var err error
	var rack2DatastoreListMap, rack3DatastoreListMap map[string]string

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
	if !multivc {
		dataCenters, err = e2eVSphere.getAllDatacenters(ctx)
	} else {
		dataCenters, err = multiVCe2eVSphere.getAllDatacentersForMultiVC(ctx)
	}
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// fetching cluster details
	// // here clientIndex=1 means VC-1 list of datastores we are fetching
	clientIndex := 0
	clusters, err := getTopologyLevel5ClusterGroupNames(masterIp, sshClientConfig, dataCenters, clientIndex)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// fetching list of datastores available in different racks
	rack1DatastoreListMap, err := getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[0], clientIndex)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if !multivc {
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[1], clientIndex)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	} else {
		// here clientIndex=1 means VC-2 list of datastores we are fetching
		clientIndex := 1
		rack2DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[0], clientIndex)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	if !multivc {
		rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[2], clientIndex)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	} else {
		// here clientIndex=1 means VC-3 list of datastores we are fetching
		clientIndex := 2
		rack3DatastoreListMap, err = getListOfDatastoresByClusterName(masterIp, sshClientConfig, clusters[0], clientIndex)
		if err != nil {
			return nil, nil, nil, nil, err
		}
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

/*
The verifyStandalonePodAffinity utility checks whether standalone pods have been scheduled on the
appropriate Availability Zone (AZ) nodes as specified in the Storage Class.
If the pods are not scheduled on the correct AZ nodes, the utility returns an error.
*/
func verifyStandalonePodAffinity(ctx context.Context, client clientset.Interface, pod *v1.Pod,
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
			nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
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

/*
volumePlacementVerificationForSts utility ensures that volumes are created on the datastore
corresponding to the specified Availability Zone (AZ) as defined in the allowed topology of the Storage Class.
If the volume placement is incorrect, the utility returns an appropriate error message.
*/

func volumePlacementVerificationForSts(ctx context.Context, client clientset.Interface, statefulset *appsv1.StatefulSet,
	namespace string, datastoreUrls []string) error {
	var isCorrectPlacement bool
	ssPods := GetListOfPodsInSts(client, statefulset)
	for _, sspod := range ssPods.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pod %s: %v", sspod.Name, err)
		}

		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				if !multivc {
					isCorrectPlacement = e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle,
						datastoreUrls)
				} else {
					isCorrectPlacement = multiVCe2eVSphere.verifyPreferredDatastoreMatchInMultiVC(pv.Spec.CSI.VolumeHandle,
						datastoreUrls)
				}
				if !isCorrectPlacement {
					return fmt.Errorf("volume provisioning has happened on the "+
						"wrong datastore for pvc %s", volumespec.PersistentVolumeClaim.ClaimName)
				}
			}
		}
	}
	return nil
}
