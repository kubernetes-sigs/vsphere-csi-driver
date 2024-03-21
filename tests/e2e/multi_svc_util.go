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
	"os/exec"
	"strings"

	"github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// getSvcComputeClusterPath method is used to get computeCluster path of clusters
func getSvcComputeClusterPath() []string {
	var computeClusterPathCmd string = govcLoginCmd()
	computeClusterPathCmd += fmt.Sprintf("govc namespace.cluster.ls")
	framework.Logf("To get compute cluster path command : %s", computeClusterPathCmd)
	result, err := exec.Command("/bin/sh", "-c", computeClusterPathCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	listPath := strings.Split(string(result), "\n")
	return listPath
}

// mountDatastoreOnClusterOrHost method is used to add a new datastore to cluster
func mountDatastoreOnClusterOrHost(datastoreName string, datastoreIP string, clusterPath string) error {
	var mountDsOnCluster string = govcLoginCmd()
	mountDsOnCluster += fmt.Sprintf("govc datastore.create -type nfs -name " + datastoreName + " -remote-host " + datastoreIP + " -remote-path /shared-nfs " + clusterPath)
	framework.Logf("mount datastore on cluster command : %s", mountDsOnCluster)
	_, err := exec.Command("/bin/sh", "-c", mountDsOnCluster).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return nil
}

// UnMountDatastoreOnCluster method is used to remove a datastore from cluster
func UnMountDatastoreFromClusterOrHost(datastoreName string, clusterOrHostPath string) error {
	UnMountDsOnCluster := govcLoginCmd() + "govc datastore.remove -ds " + datastoreName + " " + clusterOrHostPath
	framework.Logf("un-mount datastore on cluster/Host command : %s", UnMountDsOnCluster)
	_, err := exec.Command("/bin/sh", "-c", UnMountDsOnCluster).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return nil
}

// verifyPermissionForWcpStorageUser method is used to check permission of service account user
func verifyPermissionForWcpStorageUser(datastoreName string, serviceAccountUser string, role string) (bool, error) {
	permissionCheckSvcUser := govcLoginCmd() + "govc permissions.ls /WCPE2E_DC/datastore/" + datastoreName + " | grep " + serviceAccountUser + " | awk '{print $1}' "
	framework.Logf("Check permission of service account user on datastore command : %s", permissionCheckSvcUser)
	var permission string
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		result, err := exec.Command("/bin/sh", "-c", permissionCheckSvcUser).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		permission = strings.TrimSpace(strings.Split(string(result), "CNS\n")[0])
		if permission == role {
			return true, nil
		}
		return false, err
	})
	return true, waitErr
}

// verifyPasswordRotationOnSupervisor method is used to check password changes on supervisor
func verifyPasswordRotationOnSupervisor(client clientset.Interface, csiNamespace string, ctx context.Context, oldPassword string) (bool, error) {
	framework.Logf("verify password of service account user in config secret has changed")
	waitErr := wait.PollImmediate(pollTimeoutShort, pwdRotationTimeout, func() (bool, error) {
		currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		originalConf := string(currentSecret.Data["vsphere-cloud-provider.conf"])
		vsphereCfg, err := readConfigFromSecretString(originalConf)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		changedPassword := vsphereCfg.Global.Password
		framework.Logf("Pwd from Config Secret of svc: %v", changedPassword)
		if changedPassword != oldPassword {
			return true, nil
		}
		return false, err
	})
	return true, waitErr
}

// verifyAlarmFromDatacenter method is used to get alarms generated on DataCenter
func verifyAlarmFromDatacenter() error {
	alarmCmd := govcLoginCmd() + "govc events /WCPE2E_DC/ | grep 'warning'"
	framework.Logf("Get Alarms from datacenter - command : %s", alarmCmd)
	waitErr := wait.PollImmediate(healthStatusPollInterval, supervisorClusterOperationsTimeout, func() (bool, error) {
		result, err := exec.Command("/bin/sh", "-c", alarmCmd).Output()
		if err != nil {
			return false, fmt.Errorf("error fetching alarms details : %v", err)
		}
		if string(result) != "" {
			alarms := strings.Split(string(result), "\n")
			for _, alarm := range alarms {
				alarm = strings.TrimSpace(alarm)
				if strings.Contains(alarm, "Datastore not accessible to all hosts under the cluster") {
					return true, nil
				}
			}
		}
		return false, nil
	})
	return waitErr
}

// removeEsxiHostFromCluster method is used to remove esxi hosts from cluster
func removeEsxiHostFromCluster(cluster string, hostIP string) (bool, error) {
	framework.Logf("Remove Esxi Host from given cluster")
	removeHost := govcLoginCmd() + "govc object.mv /WCPE2E_DC/host/" + cluster + "/" + hostIP + " /WCPE2E_DC/host/"
	framework.Logf("remove Esxi Host from cluster command : %s", removeHost)
	_, err := exec.Command("/bin/sh", "-c", removeHost).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return true, nil
}

// moveHostToCluster method is used to move a host to cluster
func moveHostToCluster(clusterPath string, hostIP string) error {
	var moveHostCmd string = govcLoginCmd()
	moveHostCmd += fmt.Sprintf("govc cluster.mv -cluster " + clusterPath + " " + hostIP)
	framework.Logf("move host to cluster command : %s", moveHostCmd)
	_, err := exec.Command("/bin/sh", "-c", moveHostCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return nil
}

// getVcSessionIDforSupervisor method returns list of vc session id for a supervisor cluster
func getVcSessionIDforSupervisor(supervisorId string) []string {
	var getSessionIdCmd string = govcLoginCmd()
	getSessionIdCmd += fmt.Sprintf("govc session.ls | grep '" + supervisorId + "' | awk '{print $1}'")
	framework.Logf("move host to cluster command : %s", getSessionIdCmd)
	result, err := exec.Command("/bin/sh", "-c", getSessionIdCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	sessionIds := strings.Split(strings.TrimSpace(string(result)), "\n")
	return sessionIds
}

// killVcSessionIDs remove vc session id for a supervisor cluster
func killVcSessionIDs(sessionIds []string) error {
	var removeSessionId string = govcLoginCmd()
	for _, sessionId := range sessionIds {
		removeSessionIdCmd := removeSessionId + fmt.Sprintf("govc session.rm "+sessionId)
		framework.Logf("move host to cluster command : %s", removeSessionIdCmd)
		_, err := exec.Command("/bin/sh", "-c", removeSessionIdCmd).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return nil
}
