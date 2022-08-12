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
	"fmt"
	"strings"

	"github.com/onsi/ginkgo"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

var preferredDSCat string = "cns.vmware.topology-preferred-datastores"

func createPreferentialTopologyMap(topologyMapStr string) (map[string][]string,
	[]string, []string) {
	topologyMap := make(map[string][]string)
	var categories []string
	var tags []string
	topologyCategories := strings.Split(topologyMapStr, ",")
	for _, category := range topologyCategories {
		categoryVal := strings.Split(category, ":")
		key := categoryVal[0]
		value := categoryVal[1]
		categories = append(categories, key)
		tags = append(tags, value)
		values := strings.Split(categoryVal[1], ":")
		topologyMap[key] = values
	}
	return topologyMap, tags, categories
}

func createPreferentialAllowedTopolgies(topologyMapStr string) []v1.TopologySelectorLabelRequirement {
	topologyMap, _, _ := createPreferentialTopologyMap(topologyMapStr)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{}
	for key, val := range topologyMap {
		keyList := []string{key}
		allowedTopology := []v1.TopologySelectorLabelRequirement{
			{
				Key:    regionKey,
				Values: keyList,
			},
			{
				Key:    zoneKey,
				Values: val,
			},
		}
		allowedTopologies = append(allowedTopologies, allowedTopology...)
	}
	return allowedTopologies
}

func govcLoginCmd(vcAddress string, portNo string, vCenterUIUser string, vCenterUIPassword string) string {
	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';", vCenterUIUser, vCenterUIPassword, vcAddress, portNo)
	return loginCmd
}

func createPreferredDatastoreCategory(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string) {
	createDatastoreCategory := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.category.create -d 'preference datastore' -m=true " + preferredDSCat
	framework.Logf("Create category for preferred datastore: %s ", createDatastoreCategory)
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

func choosePreferredDatastore(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, datastore string, tagName string) {
	createTagCat := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.create -d '" + datastore + "' -c cns.vmware.topology-preferred-datastores " + tagName
	framework.Logf("Create category for preferred datastore: %s ", createTagCat)
	createTagCatRes, err := sshExec(sshClientConfig, masterIp, createTagCat)
	if err != nil && createTagCatRes.Code != 0 {
		fssh.LogResult(createTagCatRes)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			createTagCat, masterIp, err)
	}
	attachTagCat := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc tags.attach " + tagName + " " + "'" + datastore + "'"
	framework.Logf("Attach category to preferred datastore: %s ", attachTagCat)
	attachTagCatRes, err := sshExec(sshClientConfig, masterIp, attachTagCat)
	if err != nil && attachTagCatRes.Code != 0 {
		fssh.LogResult(attachTagCatRes)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			attachTagCat, masterIp, err)
	}
}

func getListOfAvailableDatastores(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string) map[string]string {
	datastoreListMap := make(map[string]string)
	availableDatastores := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc datastore.info | grep 'Path\\|URL'  | tr -s [:space:]"
	framework.Logf("Get list of available datastores: %s ", availableDatastores)
	result, err := sshExec(sshClientConfig, masterIp, availableDatastores)
	datastoreList := strings.Split(result.Stdout, "\n")
	for i := 0; i < len(datastoreList)-1; i = i + 2 {
		key := strings.ReplaceAll(datastoreList[i], " Path: ", "")
		value := strings.ReplaceAll(datastoreList[i+1], " URL: ", "")
		datastoreListMap[key] = value
	}
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			availableDatastores, masterIp, err)
	}
	return datastoreListMap
}

func getListOfSharedDatastoresBetweenVMs(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string) {
	listOfSharedDatastores := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc ls /VSAN-DC/vm | xargs -n1 -I% govc object.collect -s % summary.runtime.host | xargs govc datastore.info -H | grep 'Name\\|URL'"
	framework.Logf("Create category for preferred datastore: %s ", listOfSharedDatastores)
	result, err := sshExec(sshClientConfig, masterIp, listOfSharedDatastores)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		framework.Logf("couldn't execute command: %s on host: %v , error: %s",
			listOfSharedDatastores, masterIp, err)
	}
}

func getDatastoreListByCluster(vcAddress string, vCenterPort string, vCenterUIUser string,
	vCenterUIPassword string, sshClientConfig *ssh.ClientConfig,
	masterIp string, cluster string) map[string]string {
	ClusterdatastoreListMap := make(map[string]string)
	datastoreListByCluster := govcLoginCmd(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword) +
		"govc object.collect -s -d ' ' " + cluster + " host | xargs govc datastore.info -H | grep 'Path\\|URL' | tr -s [:space:]"
	framework.Logf("Get cluster datastore list : %s ", datastoreListByCluster)
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

// govc datastore.info
// govc object.collect -s -d " " /VSAN-DC/host/cluster1 host | xargs govc datastore.info -H
// govc ls /VSAN-DC/vm | xargs -n1 -I% govc object.collect -s % summary.runtime.host | xargs govc datastore.info -H

// govc ls /VSAN-DC/vm | xargs -n1 -I% govc object.collect -s % summary.runtime.host | xargs govc datastore.info -H | grep 'Name\|URL'
