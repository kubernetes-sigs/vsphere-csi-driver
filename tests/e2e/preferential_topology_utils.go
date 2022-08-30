/*
Copyright 2022 The Kubernetes Authors.

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
	"net"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	"k8s.io/utils/strings/slices"
)

const (
	stsPoll    = 10 * time.Second
	stsTimeout = 10 * time.Minute
)

var sshClientConfig *ssh.ClientConfig = &ssh.ClientConfig{
	User: "root",
	Auth: []ssh.AuthMethod{
		ssh.Password(k8sVmPasswd),
	},
	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
}

// govc login cmd
func govcLoginCmd() string {
	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';",
		e2eVSphere.Config.Global.User, e2eVSphere.Config.Global.Password,
		e2eVSphere.Config.Global.VCenterHostname, e2eVSphere.Config.Global.VCenterPort)
	return loginCmd
}

/*
getTopologyLevel5ClusterGroupNames method is used to fetch list of cluster available
in level-5 testbed
*/
func getTopologyLevel5ClusterGroupNames(masterIp string,
	dataCenter []*object.Datacenter) ([]string, error) {
	var clusterList, clusList, clusFolderTemp, clusterGroupRes []string
	var clusterFolderName string
	for i := 0; i < len(dataCenter); i++ {
		clusterFolder := govcLoginCmd() + "govc ls " + dataCenter[i].InventoryPath
		framework.Logf("cmd: %s ", clusterFolder)
		clusterFolderNameResult, err := sshExec(sshClientConfig, masterIp, clusterFolder)
		if err != nil && clusterFolderNameResult.Code != 0 {
			fssh.LogResult(clusterFolderNameResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				clusterFolder, masterIp, err)
		}
		if clusterFolderNameResult.Stdout != "" {
			clusFolderTemp = strings.Split(clusterFolderNameResult.Stdout, "\n")
		}
		for i := 0; i < len(clusFolderTemp)-1; i++ {
			if strings.Contains(clusFolderTemp[i], "host") {
				clusterFolderName = clusFolderTemp[i]
				break
			}
		}
		clusterGroup := govcLoginCmd() + "govc ls " + clusterFolderName
		framework.Logf("cmd: %s ", clusterGroup)
		clusterGroupResult, err := sshExec(sshClientConfig, masterIp, clusterGroup)
		if err != nil && clusterGroupResult.Code != 0 {
			fssh.LogResult(clusterGroupResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				clusterGroup, masterIp, err)
		}
		if clusterGroupResult.Stdout != "" {
			clusterGroupRes = strings.Split(clusterGroupResult.Stdout, "\n")
		}
		cluster := govcLoginCmd() + "govc ls " + clusterGroupRes[0] + " | sort"
		framework.Logf("cmd: %s ", cluster)
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

/*
attachTagToPreferredDatastore method is used to attach the  preferred tag to the
datastore chosen for volume provisioning
*/
func attachTagToPreferredDatastore(masterIp string, datastore string, tagName string) error {
	attachTagCat := govcLoginCmd() +
		"govc tags.attach -c " + preferredDSCat + " " + tagName + " " + "'" + datastore + "'"
	framework.Logf("cmd to attach tag to preferred datastore: %s ", attachTagCat)
	attachTagCatRes, err := sshExec(sshClientConfig, masterIp, attachTagCat)
	if err != nil && attachTagCatRes.Code != 0 {
		fssh.LogResult(attachTagCatRes)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			attachTagCat, masterIp, err)
	}
	return nil
}

/* detachTagCreatedOnPreferredDatastore is used to detach the tag attched to preferred datastore */
func detachTagCreatedOnPreferredDatastore(masterIp string, datastore string, tagName string) error {
	detachTagCat := govcLoginCmd() +
		"govc tags.detach -c " + preferredDSCat + " " + tagName + " " + "'" + datastore + "'"
	framework.Logf("cmd to detach the tag assigned to preferred datastore: %s ", detachTagCat)
	detachTagCatRes, err := sshExec(sshClientConfig, masterIp, detachTagCat)
	if err != nil && detachTagCatRes.Code != 0 {
		fssh.LogResult(detachTagCatRes)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			detachTagCat, masterIp, err)
	}
	return nil
}

/*
getListOfSharedDatastoresBetweenVMs method is used to fetch the list of datatsores accessible
to all vms or shared across entire k8s cluster
*/
func getListOfSharedDatastoresBetweenVMs(masterIp string,
	dataCenter []*object.Datacenter) (map[string]string, error) {
	var clusFolderTemp []string
	var clusterFolderName string
	shareddatastoreListMap := make(map[string]string)
	for i := 0; i < len(dataCenter); i++ {
		clusterFolder := govcLoginCmd() + "govc ls " + dataCenter[i].InventoryPath
		framework.Logf("cmd: %s ", clusterFolder)
		clusterFolderNameResult, err := sshExec(sshClientConfig, masterIp, clusterFolder)
		if err != nil && clusterFolderNameResult.Code != 0 {
			fssh.LogResult(clusterFolderNameResult)
			return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				clusterFolder, masterIp, err)
		}
		if clusterFolderNameResult.Stdout != "" {
			clusFolderTemp = strings.Split(clusterFolderNameResult.Stdout, "\n")
		}
		for i := 0; i < len(clusFolderTemp)-1; i++ {
			if strings.Contains(clusFolderTemp[i], "vm") {
				clusterFolderName = clusFolderTemp[i]
				break
			}
		}
	}
	listOfSharedDatastores := govcLoginCmd() +
		"govc ls " + clusterFolderName + " | xargs -n1 -I% govc object.collect -s % summary.runtime.host | " +
		"xargs govc datastore.info -H | grep 'Path\\|URL' | tr -s [:space:]"
	framework.Logf("cmd for fetching list of shared datastores: %s ", listOfSharedDatastores)
	result, err := sshExec(sshClientConfig, masterIp, listOfSharedDatastores)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			listOfSharedDatastores, masterIp, err)
	}
	sharedDatastoreList := strings.Split(result.Stdout, "\n")
	for i := 0; i < len(sharedDatastoreList)-1; i = i + 2 {
		key := strings.ReplaceAll(sharedDatastoreList[i], " Path: ", "")
		value := strings.ReplaceAll(sharedDatastoreList[i+1], " URL: ", "")
		shareddatastoreListMap[key] = value
	}
	return shareddatastoreListMap, nil
}

/*
getListOfDatastoresByClusterName method is used to fetch the list of datastores accessible to
specific cluster
*/
func getListOfDatastoresByClusterName(masterIp string, cluster string) (map[string]string, error) {
	ClusterdatastoreListMap := make(map[string]string)
	datastoreListByCluster := govcLoginCmd() +
		"govc object.collect -s -d ' ' " + cluster + " host | xargs govc datastore.info -H | " +
		"grep 'Path\\|URL' | tr -s [:space:]"
	framework.Logf("cmd : %s ", datastoreListByCluster)
	result, err := sshExec(sshClientConfig, masterIp, datastoreListByCluster)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			datastoreListByCluster, masterIp, err)
	}
	datastoreList := strings.Split(result.Stdout, "\n")
	for i := 0; i < len(datastoreList)-1; i = i + 2 {
		key := strings.ReplaceAll(datastoreList[i], " Path: ", "")
		value := strings.ReplaceAll(datastoreList[i+1], " URL: ", "")
		ClusterdatastoreListMap[key] = value
	}

	return ClusterdatastoreListMap, nil
}

/*
verifyVolumeProvisioningForStatefulSet is used to check whether the volume is provisioned on the
chosen preferred datastore or not for statefulsets
*/
func verifyVolumeProvisioningForStatefulSet(ctx context.Context,
	client clientset.Interface, statefulset *appsv1.StatefulSet,
	namespace string, datastoreNames []string, datastoreListMap map[string]string,
	multipleAllowedTopology bool, parallelStatefulSetCreation bool) error {
	counter := 0
	stsPodCount := 0
	var isDatastoreMatchFound bool
	var ssPodsBeforeScaleDown *v1.PodList
	if parallelStatefulSetCreation {
		ssPodsBeforeScaleDown = GetListOfPodsInSts(client, statefulset)
	} else {
		ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset)
	}
	stsPodCount = len(ssPodsBeforeScaleDown.Items)
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s",
					pv.Spec.CSI.VolumeHandle))
				for i := 0; i < len(datastoreNames); i++ {
					if val, ok := datastoreListMap[datastoreNames[i]]; ok {
						isDatastoreMatchFound = e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, val)
						if isDatastoreMatchFound {
							framework.Logf("volume %s is created on the chosen preferred datastore %v",
								pv.Spec.CSI.VolumeHandle, val)
							counter = counter + 1
							break
						}
					}
				}
			}
		}
	}
	if len(datastoreNames) == 1 && counter != stsPodCount && !multipleAllowedTopology {
		return fmt.Errorf("volume is provisioned on the wrong datastore")
	} else if len(datastoreNames) == 2 && counter != stsPodCount && !multipleAllowedTopology {
		return fmt.Errorf("volume is provisioned on the wrong datastore")
	} else if len(datastoreNames) == 3 && counter != stsPodCount && !multipleAllowedTopology {
		return fmt.Errorf("volume is provisioned on the wrong datastore")
	} else if len(datastoreNames) == 4 && counter != stsPodCount && !multipleAllowedTopology {
		return fmt.Errorf("volume is provisioned on the wrong datastore")
	} else if counter != stsPodCount && multipleAllowedTopology && counter != 0 {
		framework.Logf("Few volume is provisioned on some other datastore due to multiple " +
			"allowed topology set or no topology set in the Storage Class")
	}
	return nil
}

/*
verifyVolumeProvisioningForStandalonePods is used to check whether the volume is provisioned on the
chosen preferred datastore or not for standalone pods
*/
func verifyVolumeProvisioningForStandalonePods(ctx context.Context,
	client clientset.Interface, pod *v1.Pod,
	namespace string, datastoreNames []string, datastoreListMap map[string]string) {
	var flag bool = false
	for _, volumespec := range pod.Spec.Volumes {
		if volumespec.PersistentVolumeClaim != nil {
			pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s",
				pv.Spec.CSI.VolumeHandle))
			for i := 0; i < len(datastoreNames); i++ {
				if val, ok := datastoreListMap[datastoreNames[i]]; ok {
					res := e2eVSphere.verifyPreferredDatastoreMatch(pv.Spec.CSI.VolumeHandle, val)
					if res {
						framework.Logf("volume %v is created on preferred ds", val)
						flag = true
					}
				}
			}
			if !flag {
				err := fmt.Errorf("volume %s is provisioned on the wrong datastore", pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}
	}
}

/*
tagSameDatastoreAsPreferenceToDifferentRacks method is used to assign same preferred datatsore
to another rack in k8s cluster
*/
func tagSameDatastoreAsPreferenceToDifferentRacks(masterIp string, zoneValue string,
	itr int, datastoreNames []string) error {
	i := 0
	for j := 0; j < len(datastoreNames); j++ {
		i = i + 1
		err := attachTagToPreferredDatastore(masterIp, datastoreNames[j], zoneValue)
		if err != nil {
			return err
		}
		if i == itr {
			break
		}
	}
	return nil
}

/*
tagPreferredDatastore method is used to tag the datastore which is chosen for volume provisioning
*/
func tagPreferredDatastore(masterIp string, zoneValue string, itr int,
	datastoreListMap map[string]string, datastoreNames []string) ([]string, error) {
	var preferredDatastorePaths []string
	i := 0
	if datastoreNames == nil {
		for dsName := range datastoreListMap {
			i = i + 1
			preferredDatastorePaths = append(preferredDatastorePaths, dsName)
			err := attachTagToPreferredDatastore(masterIp, dsName, zoneValue)
			if err != nil {
				return preferredDatastorePaths, err
			}
			if i == itr {
				break
			}
		}
	}
	if datastoreNames != nil {
		for dsName := range datastoreListMap {
			if !slices.Contains(datastoreNames, dsName) {
				preferredDatastorePaths = append(preferredDatastorePaths, dsName)
				err := attachTagToPreferredDatastore(masterIp, dsName, zoneValue)
				if err != nil {
					return preferredDatastorePaths, err
				}
				i = i + 1
			}
			if i == itr {
				break
			}
		}
	}
	return preferredDatastorePaths, nil
}

/* createMultipleStatefulSetInSameNS method triggers multiple sts creation in a given namespace */
func createMultipleStatefulSetInSameNS(ns string, ss *appsv1.StatefulSet, c clientset.Interface,
	stsReplicasInNs int32) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
		ss.Namespace, ss.Name, *(ss.Spec.Replicas), ss.Spec.Selector))
	_, err := c.AppsV1().StatefulSets(ns).Create(ctx, ss, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	fss.WaitForRunningAndReady(c, stsReplicasInNs, ss)
}

// // restartCSIDriver method restarts the csi driver
// func restartCSIDriver(ctx context.Context, client clientset.Interface, namespace string,
// 	csiReplicas int32) (bool, error) {
// 	isServiceStopped, err := stopCSIPods(ctx, client)
// 	if err != nil {
// 		return isServiceStopped, err
// 	}
// 	isServiceStarted, err := startCSIPods(ctx, client, csiReplicas)
// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
// 	if err != nil {
// 		return isServiceStarted, err
// 	}
// 	time.Sleep(defaultWaitTime)
// 	return true, nil
// }

// restartCSIDriver method restarts the csi driver
func restartCSIDriver(ctx context.Context, client clientset.Interface, namespace string,
	csiReplicas int32) {
	framework.Logf("Stopping CSI driver")
	err := updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Starting CSI driver")
	err = updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix, namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

/*
setPreferredDatastoreTimeInterval method is used to set the time interval at which preferred
datastores are refreshed in the environment
*/
func setPreferredDatastoreTimeInterval(client clientset.Interface, ctx context.Context,
	csiNamespace string, namespace string, csiReplicas int32) {
	currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	originalConf := string(currentSecret.Data[vSphereCSIConf])
	vsphereCfg, err := readConfigFromSecretString(originalConf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vsphereCfg.Global.CSIFetchPreferredDatastoresIntervalInMin = preferredDatastoreRefreshTimeInterval
	modifiedConf, err := writeConfigToSecretString(vsphereCfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Updating the secret to reflect new changes")
	currentSecret.Data[vSphereCSIConf] = []byte(modifiedConf)
	_, err = client.CoreV1().Secrets(csiNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
}

// CreateStatefulSet creates a StatefulSet from the manifest at manifestPath in the given namespace.
func getNonSharedDatastoresInCluster(ClusterdatastoreListMap map[string]string,
	shareddatastoreListMap map[string]string) map[string]string {
	NonShareddatastoreListMap := make(map[string]string)
	for ClusterDsName, clusterDsVal := range ClusterdatastoreListMap {
		if _, ok := shareddatastoreListMap[ClusterDsName]; ok {
		} else {
			NonShareddatastoreListMap[ClusterDsName] = clusterDsVal
		}
	}
	return NonShareddatastoreListMap
}

// This method is used to delete the tag created for preferred datastore
func deleteTagCreatedForPreferredDatastore(masterIp string, tagName []string) error {
	for i := 0; i < len(tagName); i++ {
		deleteTagCat := govcLoginCmd() +
			"govc tags.rm -f -c " + preferredDSCat + " " + tagName[i]
		framework.Logf("Deleting tag created for preferred datastore: %s ", deleteTagCat)
		deleteTagCatRes, err := sshExec(sshClientConfig, masterIp, deleteTagCat)
		if err != nil && deleteTagCatRes.Code != 0 {
			fssh.LogResult(deleteTagCatRes)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				deleteTagCat, masterIp, err)
		}
	}
	return nil
}

// This method is used to create tag required for choosing preferred datastore
func createTagForPreferredDatastore(masterIp string, tagName []string) error {
	for i := 0; i < len(tagName); i++ {
		createTagCat := govcLoginCmd() +
			"govc tags.create -d '" + preferredTagDesc + "' -c " + preferredDSCat + " " + tagName[i]
		framework.Logf("Creating tag for preferred datastore: %s ", createTagCat)
		createTagCatRes, err := sshExec(sshClientConfig, masterIp, createTagCat)
		if err != nil && createTagCatRes.Code != 0 {
			fssh.LogResult(createTagCatRes)
			return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				createTagCat, masterIp, err)
		}
	}
	return nil
}

/*
getListOfDatastoresByClusterName method is used to fetch the list of datastores accessible to
specific cluster
*/
func getListOfAvailableDatastores(masterIp string) (map[string]string, error) {
	allDatastoreListMap := make(map[string]string)
	allDatastore := govcLoginCmd() +
		"govc datastore.info | grep 'Path\\|URL' | tr -s [:space:] "
	framework.Logf("cmd : %s ", allDatastore)
	result, err := sshExec(sshClientConfig, masterIp, allDatastore)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		return nil, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			allDatastore, masterIp, err)
	}
	datastoreList := strings.Split(result.Stdout, "\n")
	for i := 0; i < len(datastoreList)-1; i = i + 2 {
		key := strings.ReplaceAll(datastoreList[i], " Path: ", "")
		value := strings.ReplaceAll(datastoreList[i+1], " URL: ", "")
		allDatastoreListMap[key] = value
	}

	return allDatastoreListMap, nil
}

func DeleteAllStsInGivenNamespace(c clientset.Interface, ns string) {
	ssList, err := c.AppsV1().StatefulSets(ns).List(context.TODO(),
		metav1.ListOptions{LabelSelector: labels.Everything().String()})
	framework.ExpectNoError(err)
	errList := []string{}
	for i := range ssList.Items {
		ss := &ssList.Items[i]
		var err error
		if ss, err = scaleSts(c, ss, 0); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
		WaitForStatefulSetStatusReplicas(c, ss, 0)
		framework.Logf("Deleting statefulset %v", ss.Name)
		if err := c.AppsV1().StatefulSets(ss.Namespace).Delete(context.TODO(), ss.Name,
			metav1.DeleteOptions{OrphanDependents: new(bool)}); err != nil {
			errList = append(errList, fmt.Sprintf("%v", err))
		}
	}
	pvNames := sets.NewString()
	pvcPollErr := wait.PollImmediate(stsPoll, stsTimeout, func() (bool, error) {
		pvcList, err := c.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(),
			metav1.ListOptions{LabelSelector: labels.Everything().String()})
		if err != nil {
			framework.Logf("WARNING: Failed to list pvcs, retrying %v", err)
			return false, nil
		}
		for _, pvc := range pvcList.Items {
			pvNames.Insert(pvc.Spec.VolumeName)
			framework.Logf("Deleting pvc: %v with volume %v", pvc.Name, pvc.Spec.VolumeName)
			if err := c.CoreV1().PersistentVolumeClaims(ns).Delete(context.TODO(), pvc.Name,
				metav1.DeleteOptions{}); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if pvcPollErr != nil {
		errList = append(errList, "timeout waiting for pvc deletion.")
	}

	pollErr := wait.PollImmediate(stsPoll, stsTimeout, func() (bool, error) {
		pvList, err := c.CoreV1().PersistentVolumes().List(context.TODO(),
			metav1.ListOptions{LabelSelector: labels.Everything().String()})
		if err != nil {
			framework.Logf("WARNING: Failed to list pvs, retrying %v", err)
			return false, nil
		}
		waitingFor := []string{}
		for _, pv := range pvList.Items {
			if pvNames.Has(pv.Name) {
				waitingFor = append(waitingFor, fmt.Sprintf("%v: %+v", pv.Name, pv.Status))
			}
		}
		if len(waitingFor) == 0 {
			return true, nil
		}
		framework.Logf("Still waiting for pvs of statefulset to disappear:\n%v", strings.Join(waitingFor, "\n"))
		return false, nil
	})
	if pollErr != nil {
		errList = append(errList, fmt.Sprintf("Timeout waiting for pv provisioner to delete pvs, "+
			"this might mean the test leaked pvs."))
	}
	if len(errList) != 0 {
		framework.ExpectNoError(fmt.Errorf("%v", strings.Join(errList, "\n")))
	}
}

func scaleSts(c clientset.Interface, ss *appsv1.StatefulSet, count int32) (*appsv1.StatefulSet, error) {
	name := ss.Name
	ns := ss.Namespace

	framework.Logf("Scaling statefulset %s to %d", name, count)
	ss = updateSts(c, ns, name, func(ss *appsv1.StatefulSet) { *(ss.Spec.Replicas) = count })

	var statefulPodList *v1.PodList
	pollErr := wait.PollImmediate(stsPoll, stsTimeout, func() (bool, error) {
		statefulPodList = GetListOfPodsInSts(c, ss)
		if int32(len(statefulPodList.Items)) == count {
			return true, nil
		}
		return false, nil
	})
	if pollErr != nil {
		unhealthy := []string{}
		for _, statefulPod := range statefulPodList.Items {
			delTs, phase, readiness := statefulPod.DeletionTimestamp, statefulPod.Status.Phase,
				podutils.IsPodReady(&statefulPod)
			if delTs != nil || phase != v1.PodRunning || !readiness {
				unhealthy = append(unhealthy, fmt.Sprintf("%v: deletion %v, phase %v, "+
					"readiness %v", statefulPod.Name, delTs, phase, readiness))
			}
		}
		return ss, fmt.Errorf("failed to scale statefulset to %d in %v. Remaining pods:\n%v", count,
			stsTimeout, unhealthy)
	}
	return ss, nil
}

func WaitForStatefulSetStatusReplicas(c clientset.Interface, ss *appsv1.StatefulSet, expectedReplicas int32) {
	framework.Logf("Waiting for statefulset status.replicas updated to %d", expectedReplicas)

	ns, name := ss.Namespace, ss.Name
	pollErr := wait.PollImmediate(stsPoll, stsTimeout,
		func() (bool, error) {
			ssGet, err := c.AppsV1().StatefulSets(ns).Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if ssGet.Status.ObservedGeneration < ss.Generation {
				return false, nil
			}
			if ssGet.Status.Replicas != expectedReplicas {
				framework.Logf("Waiting for stateful set status.replicas to become %d,"+
					"currently %d", expectedReplicas, ssGet.Status.Replicas)
				return false, nil
			}
			return true, nil
		})
	if pollErr != nil {
		framework.Failf("Failed waiting for stateful set status.replicas updated to %d: %v", expectedReplicas, pollErr)
	}
}

func powerOnPreferredDatastore(datastoreToPowerOn string, opName string) {
	err := datastoreNimbusOps(tbinfo.user, tbinfo.location, tbinfo.podname, datastoreToPowerOn, opName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = waitForHostToBeUp(datastoreToPowerOn)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func powerOffPreferredDatastore(ctx context.Context, vs *vSphere, opName string) string {
	dsName := ""
	for _, dsInfo := range tbinfo.datastores {
		dsName = dsInfo["vmName"]
		err := datastoreNimbusOps(tbinfo.user, tbinfo.location, tbinfo.podname, dsName, opName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitForDataStoreToBeDown(dsInfo["ip"])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return dsName
}

// waitForHostToBeDown wait for host to be down
func waitForDataStoreToBeDown(ip string) error {
	framework.Logf("checking status of %s", ip)
	gomega.Expect(ip).NotTo(gomega.BeNil())
	gomega.Expect(ip).NotTo(gomega.BeEmpty())
	waitErr := wait.Poll(poll*2, pollTimeoutShort*2, func() (bool, error) {
		_, err := net.DialTimeout("tcp", ip+":22", poll)
		if err == nil {
			framework.Logf("datastore is reachable")
			return false, nil
		}
		framework.Logf("datatsore is now unreachable. Error: %s", err.Error())
		return true, nil
	})
	return waitErr
}
