/*
Copyright 2021 The Kubernetes Authors.

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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

// preferred datastore category
var preferredDSCat string = "cns.vmware.topology-preferred-datastores"
var tagDesc = "preferred datastore tag"

// govc login cmd
func govcLoginCmd(vcAddress string, portNo string, vCenterUIUser string, vCenterUIPassword string) string {
	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';", vCenterUIUser, vCenterUIPassword, vcAddress, portNo)
	return loginCmd
}

// This util method creates category required for choosing datastore as a preference for volume provisioning
func createPreferredDatastoreCategory(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string) {
	createDatastoreCategory := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.category.create -d 'preference datastore' -m=true " + preferredDSCat
	framework.Logf("Creating category for preferred datastore: %s ", createDatastoreCategory)
	result, err := sshExec(sshClientConfig, masterIp, createDatastoreCategory)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			createDatastoreCategory, masterIp, err)
	}
}

// getDataCenterDetails method is used to fetch data center details
func getDataCenterDetails(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string) ([]string, error) {
	var dataCenterList []string
	// fetch datacenter details
	ginkgo.By("Executing command for fetching data center details")
	dataCenter := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) + "govc datacenter.info | grep -i Name | awk '{print $2}'"
	framework.Logf("Fetch dataCenter: %s ", dataCenter)
	dcResult, err := sshExec(sshClientConfig, masterIp, dataCenter)
	if err != nil && dcResult.Code != 0 {
		fssh.LogResult(dcResult)
		return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			dataCenter, masterIp, err)
	}
	dcList := strings.Split(dcResult.Stdout, "\n")
	for i := 0; i < len(dcList)-1; i++ {
		dataCenterList = append(dataCenterList, dcList[i])

	}
	return dataCenterList, nil
}

// getDataCenterDetails method is used to fetch cluster details
func getClusterDetails(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, dataCenter []string) ([]string, error) {
	var clusterList []string
	var clusList []string
	// fetch cluster details
	ginkgo.By("Executing command for fetching cluster details")
	for i := 0; i < len(dataCenter); i++ {
		cluster := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) + "govc ls /" + dataCenter[i] + "/host | sort"
		framework.Logf("Fetch Cluster: %s ", cluster)
		clusterResult, err := sshExec(sshClientConfig, masterIp, cluster)
		if err != nil && clusterResult.Code != 0 {
			fssh.LogResult(clusterResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				cluster, masterIp, err)
		}
		if clusterResult.Stdout != "" {
			clusListTemp := strings.Split(clusterResult.Stdout, "\n")
			clusList = append(clusList, clusListTemp...)
		}
		for i := 0; i < len(clusList)-1; i++ {
			clusterList = append(clusterList, clusList[i])
		}
		clusList = nil
	}
	return clusterList, nil
}

// This method is used to create tag required for choosing preferred datastore
func createTagForPreferredDatastore(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, tagName string) {
	createTagCat := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.create -d '" + tagDesc + "' -c cns.vmware.topology-preferred-datastores " + tagName
	framework.Logf("Creating tag for preferred datastore: %s ", createTagCat)
	createTagCatRes, err := sshExec(sshClientConfig, masterIp, createTagCat)
	if err != nil && createTagCatRes.Code != 0 {
		fssh.LogResult(createTagCatRes)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			createTagCat, masterIp, err)
	}
}

// This method is used to attach the created tag to preferred datastore
func attachTagToPreferredDatastore(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, datastore string, tagName string) {
	attachTagCat := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.attach -c " + preferredDSCat + " " + tagName + " " + "'" + datastore + "'"
	framework.Logf("Attaching tag to preferred datastore: %s ", attachTagCat)
	attachTagCatRes, err := sshExec(sshClientConfig, masterIp, attachTagCat)
	if err != nil && attachTagCatRes.Code != 0 {
		fssh.LogResult(attachTagCatRes)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			attachTagCat, masterIp, err)
	}
}

// This method is used to delete the tag created for preferred datastore
func deleteTagCreatedForPreferredDatastore(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, tagName string) {
	deleteTagCat := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.rm -f -c " + preferredDSCat + " " + tagName
	framework.Logf("Deleting tag created for preferred datastore: %s ", deleteTagCat)
	deleteTagCatRes, err := sshExec(sshClientConfig, masterIp, deleteTagCat)
	if err != nil && deleteTagCatRes.Code != 0 {
		fssh.LogResult(deleteTagCatRes)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			deleteTagCat, masterIp, err)
	}
}

// This method is used to detach the tag attched to preferred datastore
func detachTagCreatedOnPreferredDatastore(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, datastore string, tagName string) {
	detachTagCat := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.detach -c " + preferredDSCat + " " + tagName + " " + "'" + datastore + "'"
	framework.Logf("Detach tag attached to preferred datastore: %s ", detachTagCat)
	detachTagCatRes, err := sshExec(sshClientConfig, masterIp, detachTagCat)
	if err != nil && detachTagCatRes.Code != 0 {
		fssh.LogResult(detachTagCatRes)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			detachTagCatRes, masterIp, err)
	}
}

// This method is used to fetch the datastores list shared across vm's
func getListOfSharedDatastoresBetweenVMs(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string) map[string]string {
	shareddatastoreListMap := make(map[string]string)
	listOfSharedDatastores := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc ls /VSAN-DC/vm | xargs -n1 -I% govc object.collect -s % summary.runtime.host | " +
		"xargs govc datastore.info -H | grep 'Path\\|URL' | tr -s [:space:]"
	framework.Logf("List of datastores shared between vm's: %s ", listOfSharedDatastores)
	result, err := sshExec(sshClientConfig, masterIp, listOfSharedDatastores)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			listOfSharedDatastores, masterIp, err)
	}
	sharedDatastoreList := strings.Split(result.Stdout, "\n")
	for i := 0; i < len(sharedDatastoreList)-1; i = i + 2 {
		key := strings.ReplaceAll(sharedDatastoreList[i], " Path: ", "")
		value := strings.ReplaceAll(sharedDatastoreList[i+1], " URL: ", "")
		shareddatastoreListMap[key] = value
	}
	return shareddatastoreListMap
}

// This method is used to fetch the datastores list cluster wise
func getDatastoreListByCluster(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, cluster string) map[string]string {
	ClusterdatastoreListMap := make(map[string]string)
	datastoreListByCluster := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc object.collect -s -d ' ' " + cluster + " host | xargs govc datastore.info -H | grep 'Path\\|URL' | tr -s [:space:]"
	framework.Logf("Datastore list by cluster : %s ", datastoreListByCluster)
	result, err := sshExec(sshClientConfig, masterIp, datastoreListByCluster)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			datastoreListByCluster, masterIp, err)
	}
	datastoreList := strings.Split(result.Stdout, "\n")
	for i := 0; i < len(datastoreList)-1; i = i + 2 {
		key := strings.ReplaceAll(datastoreList[i], " Path: ", "")
		value := strings.ReplaceAll(datastoreList[i+1], " URL: ", "")
		ClusterdatastoreListMap[key] = value
	}
	return ClusterdatastoreListMap
}

// restartCSIDriver method restarts the csi driver
func restartCSIDriver(ctx context.Context, client clientset.Interface, namespace string,
	csiReplicas int32) (bool, error) {
	isServiceStopped, err := stopCSIPods(ctx, client)
	if err != nil {
		return isServiceStopped, err
	}
	isServiceStarted, err := startCSIPods(ctx, client, csiReplicas)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if err != nil {
		return isServiceStarted, err
	}
	time.Sleep(3 * time.Minute)
	return true, nil
}

func verifyVolumeProvisioningForStatefulSet(ctx context.Context,
	client clientset.Interface, statefulset *appsv1.StatefulSet,
	namespace string, datastoreNames []string, datastoreListMap map[string]string) {
	var flag bool = false
	ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
				queryResult, err := e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				if len(queryResult.Volumes) == 0 {
					err = fmt.Errorf("error: QueryCNSVolumeWithResult returned no volume")
				}
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Verify if volume is provisioned on the specified datastore")
				for i := 0; i < len(datastoreNames); i++ {
					if val, ok := datastoreListMap[datastoreNames[i]]; ok {
						if queryResult.Volumes[0].DatastoreUrl == val {
							framework.Logf("Volume %s is provisioned on the preferred datastore", pv.Spec.CSI.VolumeHandle)
							flag = true
						}
					} else {
						err = fmt.Errorf("no preferred datatsore found")
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
				if !flag {
					err = fmt.Errorf("volume %s is provisioned on the wrong datastore", pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
	}
}

func verifyVolumeProvisioningForStandalonePods(ctx context.Context,
	client clientset.Interface, pod *v1.Pod,
	namespace string, datastoreNames []string, ClusterdatastoreListMap map[string]string) {
	var flag bool = false
	for _, volumespec := range pod.Spec.Volumes {
		if volumespec.PersistentVolumeClaim != nil {
			pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", pv.Spec.CSI.VolumeHandle))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if len(queryResult.Volumes) == 0 {
				err = fmt.Errorf("error: QueryCNSVolumeWithResult returned no volume")
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying disk is created on the specified datastore")
			for i := 0; i < len(datastoreNames); i++ {
				if val, ok := ClusterdatastoreListMap[datastoreNames[i]]; ok {
					if queryResult.Volumes[0].DatastoreUrl == val {
						framework.Logf("Volume is provisioned on the preferred datastore")
						flag = true
					}
				} else {
					err = fmt.Errorf("no preferred datatsore found")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			if !flag {
				err = fmt.Errorf("volume is provisioned on the wrong datastore")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

// func getListOfAvailableDatastores(vcAddress string, vCenterPort string, vCenterUIUser string,
// 	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
// 	masterIp string) map[string]string {
// 	datastoreListMap := make(map[string]string)
// 	availableDatastores := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
// 		"govc datastore.info | grep 'Path\\|URL'  | tr -s [:space:]"
// 	framework.Logf("Get list of available datastores: %s ", availableDatastores)
// 	result, err := sshExec(sshClientConfig, masterIp, availableDatastores)
// 	datastoreList := strings.Split(result.Stdout, "\n")
// 	for i := 0; i < len(datastoreList)-1; i = i + 2 {
// 		key := strings.ReplaceAll(datastoreList[i], " Path: ", "")
// 		value := strings.ReplaceAll(datastoreList[i+1], " URL: ", "")
// 		datastoreListMap[key] = value
// 	}
// 	if err != nil && result.Code != 0 {
// 		fssh.LogResult(result)
// 		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
// 			availableDatastores, masterIp, err)
// 	}
// 	return datastoreListMap
// }

// func createPreferentialTopologyMap(topologyMapStr string) (map[string][]string,
// 	[]string, []string) {
// 	topologyMap := make(map[string][]string)
// 	var categories []string
// 	var tags []string
// 	topologyCategories := strings.Split(topologyMapStr, ",")
// 	for _, category := range topologyCategories {
// 		categoryVal := strings.Split(category, ":")
// 		key := categoryVal[0]
// 		value := categoryVal[1]
// 		categories = append(categories, key)
// 		tags = append(tags, value)
// 		values := strings.Split(categoryVal[1], ":")
// 		topologyMap[key] = values
// 	}
// 	return topologyMap, tags, categories
// }

// func createPreferentialAllowedTopolgies(topologyMapStr string) []v1.TopologySelectorLabelRequirement {
// 	topologyMap, _, _ := createPreferentialTopologyMap(topologyMapStr)
// 	allowedTopologies := []v1.TopologySelectorLabelRequirement{}
// 	for key, val := range topologyMap {
// 		keyList := []string{key}
// 		allowedTopology := []v1.TopologySelectorLabelRequirement{
// 			{
// 				Key:    regionKey,
// 				Values: keyList,
// 			},
// 			{
// 				Key:    zoneKey,
// 				Values: val,
// 			},
// 		}
// 		allowedTopologies = append(allowedTopologies, allowedTopology...)
// 	}
// 	return allowedTopologies
// }

// // This method performs the cleanup removed the preference tag from chosen datatsore
// func RemovePreferenceOfDatastore(vcAddress string, vCenterPort string, vCenterUIUser string,
// 	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
// 	masterIp string, datastore string, tagName string) {
// 	detachTagCreatedOnPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser,
// 		vCenterUIPassword, sshClientConfig, masterIp, datastore, tagName)

// 	deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser,
// 		vCenterUIPassword, sshClientConfig, masterIp, datastore, tagName)
// }

// This method chooses the datatsore as a preference choice
func choosePreferredDatastore(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, zoneValue string, itr int, ClusterdatastoreListMap map[string]string) []string {
	var datastoreNames []string
	i := 0
	for dsName := range ClusterdatastoreListMap {
		i = i + 1
		datastoreNames = append(datastoreNames, dsName)
		attachTagToPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, dsName, zoneValue)
		if i == itr {
			break
		}
	}
	return datastoreNames
}

// This method chooses the datatsore as a preference choice
func changeDatastorePreference(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, zoneValue string, itr int,
	datastoreListMap map[string]string, datastoreNames []string) []string {
	var preferredDsList []string
	i := 0
	for dsName := range datastoreListMap {
		for j := 0; j < len(datastoreNames); j++ {
			if datastoreNames[i] != datastoreListMap[dsName] {
				i = i + 1
				preferredDsList = append(preferredDsList, dsName)
				attachTagToPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser,
					vCenterUIPassword, sshClientConfig, masterIp, dsName, zoneValue)
			}
		}
		if i == itr {
			break
		}
	}
	return preferredDsList
}

// govc datastore.info
// govc object.collect -s -d " " /VSAN-DC/host/cluster1 host | xargs govc datastore.info -H
// govc ls /VSAN-DC/vm | xargs -n1 -I% govc object.collect -s % summary.runtime.host | xargs govc datastore.info -H

// govc ls /VSAN-DC/vm | xargs -n1 -I% govc object.collect -s % summary.runtime.host | xargs govc datastore.info -H | grep 'Name\|URL'
