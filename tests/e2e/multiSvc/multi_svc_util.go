/*
Copyright 2024 The Kubernetes Authors.

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

package multiSvc

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strings"

	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// GetSvcCountAndComputeClusterPath method is used to get number of clusters and it's computeCluster path
func GetSvcCountAndComputeClusterPath(e2eTestConfig *config.E2eTestConfig) (int, []string, error) {
	computeClusterPath := vcutil.GovcLoginCmd(e2eTestConfig) + "govc namespace.cluster.ls"
	framework.Logf("To get number of compute cluster and it's path - command : %s", computeClusterPath)
	result, err := exec.Command("/bin/sh", "-c", computeClusterPath).Output()
	if err != nil {
		framework.Logf("Error: %v\n", err)
		return 0, []string{}, fmt.Errorf("couldn't execute command: %s, error: %s",
			computeClusterPath, err)
	}
	listPath := strings.Split(strings.TrimSpace(string(result)), "\n")
	sort.Strings(listPath)
	return len(listPath), listPath, nil
}

// MountNfsDatastoreOnClusterOrHost method is used to add a new datastore to cluster
func MountNfsDatastoreOnClusterOrHost(e2eTestConfig *config.E2eTestConfig, datastoreName string, datastoreIP string,
	clusterPath string) error {
	mountDsOnCluster := vcutil.GovcLoginCmd(e2eTestConfig) + "govc datastore.create -type nfs -name " + datastoreName +
		" -remote-host " + datastoreIP + " -remote-path /shared-nfs " + clusterPath
	framework.Logf("Mount datastore on cluster/host - command : %s", mountDsOnCluster)
	_, err := exec.Command("/bin/sh", "-c", mountDsOnCluster).Output()
	if err != nil {
		framework.Logf("Error: %v\n", err)
		return fmt.Errorf("couldn't execute command: %s, error: %s",
			mountDsOnCluster, err)
	}
	return nil
}

// UnMountNfsDatastoreFromClusterOrHost method is used to remove a datastore from cluster
func UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig *config.E2eTestConfig, datastoreName string,
	clusterOrHostPath string) error {
	UnMountDsOnCluster := vcutil.GovcLoginCmd(e2eTestConfig) + "govc datastore.remove -ds " + datastoreName + " " +
		clusterOrHostPath
	framework.Logf("Un-mount datastore on cluster/Host - command : %s", UnMountDsOnCluster)
	_, err := exec.Command("/bin/sh", "-c", UnMountDsOnCluster).Output()
	if err != nil {
		framework.Logf("Error: %v\n", err)
		return fmt.Errorf("couldn't execute command: %s, error: %s",
			UnMountDsOnCluster, err)
	}
	return nil
}

// VerifyPermissionForWcpStorageUser method is used to check permission of service account user
func VerifyPermissionForWcpStorageUser(ctx context.Context, e2eTestConfig *config.E2eTestConfig, entity string,
	path string, serviceAccountUser string, role string) (bool, error) {
	var permissionCheckSvcUser string = vcutil.GovcLoginCmd(e2eTestConfig)
	var grepServiceAccUser string = " | grep " + serviceAccountUser + " | awk '{print $1}' "

	switch entity {
	case "RootFolder":
		permissionCheckSvcUser += "govc permissions.ls /" + grepServiceAccUser
	case "Cluster":
		permissionCheckSvcUser += "govc permissions.ls " + path + grepServiceAccUser
	case "Datastore":
		permissionCheckSvcUser += "govc permissions.ls '" + path + "'" + grepServiceAccUser
	default:
		framework.Logf("Please pass a proper entity")
		return false, errors.New("enter a valid entity")
	}

	framework.Logf("Check permission of service account user on %s - command : %s", entity, permissionCheckSvcUser)
	var permission string
	waitErr := wait.PollUntilContextTimeout(ctx, constants.HealthStatusPollInterval, constants.PollTimeoutSixMin, true,
		func(ctx context.Context) (bool, error) {
			result, err := exec.Command("/bin/sh", "-c", permissionCheckSvcUser).Output()
			if err != nil {
				return false, err
			}
			permission = strings.TrimSpace(strings.Split(string(result), "CNS\n")[0])
			framework.Logf("Permission for wcp storgae user is : %v", permission)
			if permission == role {
				return true, nil
			}
			return false, err
		})
	return true, waitErr
}

// IsAlarmPresentOnDatacenter method is used to check if alarm is generated on a dataCenter
func IsAlarmPresentOnDatacenter(ctx context.Context, e2eTestConfig *config.E2eTestConfig, datacenter string,
	alarmToVerify string, alarmShouldExists bool) (bool, error) {
	alarmCmd := vcutil.GovcLoginCmd(e2eTestConfig) + "govc alarms " + datacenter
	framework.Logf("Get alarms from datacenter - command : %s", alarmCmd)
	waitErr := wait.PollUntilContextTimeout(ctx, constants.HealthStatusPollInterval, constants.PollTimeoutSixMin, true,
		func(ctx context.Context) (bool, error) {
			result, err := exec.Command("/bin/sh", "-c", alarmCmd).Output()
			if err != nil {
				// handling ExitError here which occurs sometimes
				if exitErr, ok := err.(*exec.ExitError); ok {
					// Get the stderr output
					stderr := string(exitErr.Stderr)
					framework.Logf("Stderr: %v", stderr)
					return false, nil
				} else {
					framework.Logf("Not an ExitError: %v", err)
					return false, fmt.Errorf("error fetching alarms details : %v", err)
				}

			}
			if string(result) != "" {
				alarms := strings.Split(string(result), "\n")
				for _, alarm := range alarms {
					alarm = strings.TrimSpace(alarm)
					// Checking if required alarm appears
					if strings.Contains(alarm, alarmToVerify) {
						if alarmShouldExists {
							framework.Logf("Required alarm Found : %s", alarm)
							return true, nil
						} else {
							// In case alarmShouldExists is false, but alarm is found
							return false, nil
						}
					}
				}
				// after above iteration, in case alarm not present and param alarmShouldExists also false
				if !alarmShouldExists {
					framework.Logf("Required alarm not found")
					return true, nil
				}
			}
			return false, nil
		})
	return true, waitErr
}

// RemoveEsxiHostFromCluster method is used to remove esxi hosts from cluster
func RemoveEsxiHostFromCluster(e2eTestConfig *config.E2eTestConfig, datacenter string, cluster string,
	hostIP string) (bool, error) {
	removeHostFromCluster := vcutil.GovcLoginCmd(e2eTestConfig) + "govc object.mv /" + datacenter + "/host/" + cluster +
		"/" + hostIP + " /" + datacenter + "/host/"
	framework.Logf("Remove an ESXi host from cluster command : %s", removeHostFromCluster)
	_, err := exec.Command("/bin/sh", "-c", removeHostFromCluster).Output()
	if err != nil {
		framework.Logf("Error: %v\n", err)
		return false, fmt.Errorf("couldn't execute command: %s, error: %s",
			removeHostFromCluster, err)
	}
	return true, nil
}

// MoveHostToCluster method is used to move a host to cluster
func MoveHostToCluster(e2eTestConfig *config.E2eTestConfig, clusterPath string, hostIP string) error {
	moveHostToCluster := vcutil.GovcLoginCmd(e2eTestConfig) + "govc cluster.mv -cluster " + clusterPath + " " + hostIP
	framework.Logf("Move a host to cluster command : %s", moveHostToCluster)
	_, err := exec.Command("/bin/sh", "-c", moveHostToCluster).Output()
	if err != nil {
		framework.Logf("Error: %v\n", err)
		return fmt.Errorf("couldn't execute command: %s, error: %s",
			moveHostToCluster, err)
	}
	return nil
}

// GetVcSessionIDsforSupervisor method returns list of vc session id for a supervisor id and returns error if any
func GetVcSessionIDsforSupervisor(e2eTestConfig *config.E2eTestConfig, supervisorId string) ([]string, error) {
	getSessionId := vcutil.GovcLoginCmd(e2eTestConfig) + "govc session.ls | grep 'csi-useragent' | grep '" +
		supervisorId + "' | awk '{print $1}'"
	framework.Logf("Get Vc session ID for cluster command : %s", getSessionId)
	result, err := exec.Command("/bin/sh", "-c", getSessionId).Output()
	if err != nil {
		return []string{}, fmt.Errorf("couldn't execute command: %s, error: %s",
			getSessionId, err)
	}
	sessionIds := strings.Split(strings.TrimSpace(string(result)), "\n")
	return sessionIds, nil
}

// KillVcSessionIDs remove vc session id for a supervisor cluster
func KillVcSessionIDs(e2eTestConfig *config.E2eTestConfig, sessionIds []string) error {
	var govcLogin string = vcutil.GovcLoginCmd(e2eTestConfig)
	for _, sessionId := range sessionIds {
		removeSessionIdCmd := govcLogin + "govc session.rm " + sessionId
		framework.Logf("Remove vc session id from cluster - command : %s", removeSessionIdCmd)
		_, err := exec.Command("/bin/sh", "-c", removeSessionIdCmd).Output()
		if err != nil {
			framework.Logf("Error: %v\n", err)
			return fmt.Errorf("couldn't execute command: %s, error: %s",
				removeSessionIdCmd, err)
		}
	}
	return nil
}

// GetSvcConfigSecretData returns data obtained fom csi config secret
// in namespace where CSI is deployed
func GetSvcConfigSecretData(client clientset.Interface, ctx context.Context, e2eTestConfig *config.E2eTestConfig,
	csiNamespace string) (config.E2eTestConfig, error) {
	var vsphereCfg config.E2eTestConfig
	currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx, constants.ConfigSecret, metav1.GetOptions{})
	if err != nil {
		return vsphereCfg, err
	}
	originalConf := string(currentSecret.Data[constants.VsphereCloudProviderConfiguration])
	vsphereCfg, err = k8testutil.ReadConfigFromSecretString(originalConf)
	if err != nil {
		return vsphereCfg, err
	}

	return vsphereCfg, nil
}

// GetDatastoreNamesFromDCs method is used to fetch datastore details from a multi-supervisor testbed
func GetDatastoreNamesFromDCs(sshClientConfig *ssh.ClientConfig, e2eTestConfig *config.E2eTestConfig,
	dataCenters []*object.Datacenter) ([]string, error) {
	var dsList, datastores []string
	framework.Logf("Fetching datastore details")
	for i := 0; i < len(dataCenters); i++ {
		ds := vcutil.GovcLoginCmd(e2eTestConfig) + "govc ls " + dataCenters[i].InventoryPath + "/datastore"
		dsResult, err := exec.Command("/bin/sh", "-c", ds).Output()
		if err != nil {
			framework.Logf("dsResult %s", string(dsResult))
			return nil, fmt.Errorf("couldn't execute command: %s , error: %s",
				ds, err)
		}
		if string(dsResult) != "" {
			dsListTemp := strings.Split(string(dsResult), "\n")
			dsList = append(dsList, dsListTemp...)
		}
		for i := 0; i < len(dsList)-1; i++ {
			datastores = append(datastores, dsList[i])
		}
		dsList = nil
	}
	return datastores, nil
}

// WaitAndCompareSessionIDList method is used to match new session ids with old session ids
func WaitAndCompareSessionIDList(ctx context.Context, e2eTestConfig *config.E2eTestConfig, supervisorId string,
	oldSessionIds []string) (bool, error) {
	var newSessionIds []string
	var err error
	var retryCount int
	framework.Logf("Old Session Ids : %s", oldSessionIds)
	// polling for current vc session ids for svc
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll*10, constants.VcSessionWaitTime, true,
		func(ctx context.Context) (bool, error) {
			newSessionIds, err = GetVcSessionIDsforSupervisor(e2eTestConfig, supervisorId)
			if err != nil {
				// If there was an error, return the error
				return false, err
			}

			if len(newSessionIds) == len(oldSessionIds) {
				retryCount++
				// retrying for 3 times to avoid any transient situation
				if retryCount < 3 {
					return false, nil
				}
				return true, nil
			}
			return false, err
		})
	framework.Logf("New Session Ids : %s", newSessionIds)
	// returns true if both sessionIds are same else false
	return reflect.DeepEqual(oldSessionIds, newSessionIds), waitErr
}
