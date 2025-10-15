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
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vsan"
	vsantypes "github.com/vmware/govmomi/vsan/types"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pkgtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	"k8s.io/pod-security-admission/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
)

const (
	filePath1  = "/mnt/volume1/file1.txt"
	filePath2  = "/mnt/volume1/file2.txt"
	accessMode = v1.ReadWriteMany
)

type FaultDomains struct {
	primarySiteHosts   []string
	secondarySiteHosts []string
	witness            string
	hostsDown          []string `default:"[]"`
	hostsPartitioned   []string `default:"[]"`
	witnessDown        string
}

var fds FaultDomains

// initialiseFdsVar initialise fds variable
func initialiseFdsVar(ctx context.Context) {
	fdMap := createFaultDomainMap(ctx, &e2eVSphere)
	hostsWithoutFD := []string{}
	for host, site := range fdMap {
		if strings.Contains(site, "rimary") {
			fds.primarySiteHosts = append(fds.primarySiteHosts, host)
		} else if strings.Contains(site, "econdary") {
			fds.secondarySiteHosts = append(fds.secondarySiteHosts, host)
		} else {
			hostsWithoutFD = append(hostsWithoutFD, host)
		}
	}

	// assuming we don't have hosts which are not part of the vsan stretched cluster in the testbed here
	gomega.Expect(len(hostsWithoutFD) == 1).To(gomega.BeTrue())
	fds.witness = hostsWithoutFD[0]

}

// siteFailureInParallel causes site Failure in multiple hosts of the site in parallel
func siteFailureInParallel(ctx context.Context, primarySite bool, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	siteFailover(ctx, primarySite)
}

// siteFailover causes a site failover by powering off hosts of the given site
func siteFailover(ctx context.Context, primarySite bool) {
	hostsToPowerOff := fds.secondarySiteHosts
	if primarySite {
		hostsToPowerOff = fds.primarySiteHosts
	}
	framework.Logf("hosts to power off: %v", hostsToPowerOff)
	powerOffHostParallel(ctx, hostsToPowerOff)
}

// powerOffHostParallel powers off given hosts
func powerOffHostParallel(ctx context.Context, hostsToPowerOff []string) {
	hostlist := ""
	for _, host := range hostsToPowerOff {
		for _, esxHost := range tbinfo.esxHosts {
			if esxHost["ip"] == host {
				hostlist += esxHost["vmName"] + " "
			}
		}
		fds.hostsDown = append(fds.hostsDown, host)
	}

	err := vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostlist, false)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, host := range hostsToPowerOff {
		err = waitForHostToBeDown(ctx, host)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// siteRestore restores a site by powering on hosts of the given site
func siteRestore(primarySite bool) {
	hostsToPowerOn := fds.secondarySiteHosts
	if primarySite {
		hostsToPowerOn = fds.primarySiteHosts
	}
	framework.Logf("hosts to power on: %v", hostsToPowerOn)
	powerOnHostParallel(hostsToPowerOn)
}

// powerOnHostParallel powers on given hosts
func powerOnHostParallel(hostsToPowerOn []string) {
	hostlist := ""
	for _, host := range hostsToPowerOn {
		for _, esxHost := range tbinfo.esxHosts {
			if esxHost["ip"] == host {
				hostlist += esxHost["vmName"] + " "
			}
		}
		fds.hostsDown = append(fds.hostsDown, host)
	}
	err := vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostlist, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, host := range hostsToPowerOn {
		err = waitForHostToBeUp(host, time.Minute*40)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// createFaultDomainMap returns the host to fault domain mapping
func createFaultDomainMap(ctx context.Context, vs *vSphere) map[string]string {
	fdMap := make(map[string]string)
	c := newClient(ctx, vs)

	datacenter := strings.Split(e2eVSphere.Config.Global.Datacenters, ",")[0]

	vsanHealthClient, err := newVsanHealthSvcClient(ctx, c.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vsanClient, err := vsan.NewClient(ctx, vsanHealthClient.vim25Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	finder := find.NewFinder(vsanHealthClient.vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	hosts, err := finder.HostSystemList(ctx, "*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if !vanillaCluster {
		hostsInVsanStretchCluster := []*object.HostSystem{}
		for _, host := range hosts {
			hostInfo := host.Common.InventoryPath
			hostIpInfo := strings.Split(hostInfo, "/")
			hostCluster := hostIpInfo[len(hostIpInfo)-2]
			if !(strings.Contains(hostCluster, "EdgeMgmtCluster") || strings.Contains(hostCluster, "mgmt")) {
				hostsInVsanStretchCluster = append(hostsInVsanStretchCluster, host)
			}
		}
		hosts = hostsInVsanStretchCluster
	}

	for _, host := range hosts {
		vsanSystem, _ := host.ConfigManager().VsanSystem(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var hostConfig *vsantypes.VsanHostConfigInfoEx
		// Wait for hosts to come out of error state and poll for hostconfig to be found
		waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeout*2, true,
			func(ctx context.Context) (bool, error) {
				hostConfig, err = vsanClient.VsanHostGetConfig(ctx, vsanSystem.Reference())
				if err == nil {
					return true, nil
				}
				if err != nil && !strings.Contains(err.Error(), "host vSAN config not found") {
					return false, fmt.Errorf("hosts are not in ready state")
				}
				return false, nil
			})
		gomega.Expect(waitErr).NotTo(gomega.HaveOccurred())
		fdMap[host.Name()] = ""
		if hostConfig.FaultDomainInfo != nil {
			fdMap[host.Name()] = hostConfig.FaultDomainInfo.Name
			framework.Logf("host: %s, site: %s", host.Name(), hostConfig.FaultDomainInfo.Name)
		}
	}

	return fdMap
}

// waitForHostToBeDown wait for host to be down
func waitForHostToBeDown(ctx context.Context, ip string) error {
	framework.Logf("checking host status of %s", ip)
	gomega.Expect(ip).NotTo(gomega.BeNil())
	gomega.Expect(ip).NotTo(gomega.BeEmpty())
	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(ip)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	waitErr := wait.PollUntilContextTimeout(ctx, poll*2, pollTimeoutShort*2, true,
		func(ctx context.Context) (bool, error) {
			_, err := net.DialTimeout("tcp", addr, poll)
			if err == nil {
				framework.Logf("host is reachable")
				return false, nil
			}
			framework.Logf("host is now unreachable. Error: %s", err.Error())
			return true, nil
		})
	return waitErr
}

// waitForAllNodes2BeReady checks whether all registered nodes are ready and all required Pods are running on them.
func waitForAllNodes2BeReady(ctx context.Context, c clientset.Interface, timeout ...time.Duration) error {
	var pollTime time.Duration
	if len(timeout) > 0 {
		pollTime = timeout[0]
	} else {
		if os.Getenv("K8S_NODES_UP_WAIT_TIME") != "" {
			k8sNodeWaitTime, err := strconv.Atoi(os.Getenv(envK8sNodesUpWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pollTime = time.Duration(k8sNodeWaitTime) * time.Minute
		} else {
			pollTime = time.Duration(defaultK8sNodesUpWaitTime) * time.Minute
		}
	}
	framework.Logf("Waiting up to %v for all nodes to be ready", pollTime)

	var notReady []v1.Node
	err := wait.PollUntilContextTimeout(ctx, poll, pollTime, true,
		func(ctx context.Context) (bool, error) {
			notReady = nil
			nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})

			if err != nil && !strings.Contains(err.Error(), "has prevented the request") &&
				!strings.Contains(err.Error(), "TLS handshake timeout") &&
				!strings.Contains(err.Error(), "dial tcp") &&
				!strings.Contains(err.Error(), "nodes is forbidden") &&
				!strings.Contains(err.Error(), "http2: client connection lost") &&
				!strings.Contains(err.Error(), "etcdserver: request timed out") &&
				!strings.Contains(err.Error(), ": EOF") {
				return false, err
			}
			for _, node := range nodes.Items {
				if !fnodes.IsConditionSetAsExpected(&node, v1.NodeReady, true) {
					notReady = append(notReady, node)
				}
				for _, node := range nodes.Items {
					if !fnodes.IsConditionSetAsExpected(&node, v1.NodeReady, true) {
						notReady = append(notReady, node)
					}
				}
			}
			return len(notReady) == 0 && err == nil, nil
		})
	if len(notReady) > 0 {
		return fmt.Errorf("not ready nodes: %v", notReady)
	}
	return err
}

// wait4AllK8sNodesToBeUp wait for all k8s nodes to be reachable
func wait4AllK8sNodesToBeUp(k8sNodes *v1.NodeList) {
	var nodeIp string
	for _, node := range k8sNodes.Items {
		addrs := node.Status.Addresses
		for _, addr := range addrs {
			if addr.Type == v1.NodeInternalIP && (net.ParseIP(addr.Address)).To4() != nil {
				nodeIp = addr.Address
			}
		}
		err := waitForHostToBeUp(nodeIp, time.Minute*40)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// deletePodsInParallel deletes pods in a given namespace in parallel
func deletePodsInParallel(ctx context.Context, client clientset.Interface, namespace string,
	pods []*v1.Pod, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for _, pod := range pods {
		fpod.DeletePodOrFail(ctx, client, namespace, pod.Name)
	}
}

// createPvcInParallel creates number of PVC in a given namespace in parallel
func createPvcInParallel(ctx context.Context, client clientset.Interface, namespace string,
	diskSize string, sc *storagev1.StorageClass,
	ch chan *v1.PersistentVolumeClaim, lock *sync.Mutex, wg *sync.WaitGroup, volumeOpsScale int) {
	var err error
	var pvc *v1.PersistentVolumeClaim
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for i := 0; i < volumeOpsScale; i++ {

		//If file vanilla it uses ReadWriteMany
		if rwxAccessMode {
			pvc, err = createPVC(ctx, client, namespace, nil, diskSize, sc, accessMode)
		} else {
			pvc, err = createPVC(ctx, client, namespace, nil, diskSize, sc, "")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		lock.Lock()
		ch <- pvc
		lock.Unlock()
	}
}

// siteNetworkFailure chooses a site to create or remove network failure
func siteNetworkFailure(primarySite bool, removeNetworkFailure bool) {
	hosts := fds.secondarySiteHosts
	if primarySite {
		hosts = fds.primarySiteHosts
	}
	if removeNetworkFailure {
		framework.Logf("hosts to remove network failure on: %v", hosts)
		toggleNetworkFailureParallel(hosts, false)
		fds.hostsPartitioned = []string{}
	} else {
		framework.Logf("hosts to cause network failure on: %v", hosts)
		toggleNetworkFailureParallel(hosts, true)
		fds.hostsPartitioned = hosts
	}
}

// waitForPodsToBeInErrorOrRunning polls for pod to be in error or running state
func waitForPodsToBeInErrorOrRunning(ctx context.Context, c clientset.Interface, podName, namespace string,
	timeout time.Duration) error {
	waitErr := wait.PollUntilContextTimeout(ctx, poll, timeout, true,
		func(ctx context.Context) (bool, error) {
			pod, err := c.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			framework.Logf("Pod is in phase: %v", pod.Status.Phase)
			switch pod.Status.Phase {
			// v1.PodSucceeded is for pods in ExitCode:0 state.
			// Standalone pods are in ExitCode:0 or Running state after site failure.
			case v1.PodRunning, v1.PodSucceeded, v1.PodPending:
				framework.Logf("Pod %v is in state %v", podName, pod.Status.Phase)
				return true, nil
			}
			return false, nil
		})
	return waitErr
}

// runCmdOnHostsInParallel runs command on multiple ESX in parallel
func runCmdOnHostsInParallel(hostIP string, sshCmd string, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	op, err := runCommandOnESX("root", hostIP, sshCmd)
	framework.Logf("%q", op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// toggleNetworkFailureParallel causes or removes network failure on a particular site
func toggleNetworkFailureParallel(hosts []string, causeNetworkFailure bool) {
	var wg sync.WaitGroup
	if causeNetworkFailure {
		framework.Logf("Creating a Network Failure")
		sshCmd := "localcli network firewall set --enabled true;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 0 --ruleset-id cmmds;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 0 --ruleset-id rdt;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 0 --ruleset-id fdm;"

		wg.Add(len(hosts))
		for _, host := range hosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()
		sshCmd = "vsish -e set /vmkModules/esxfw/globaloptions 1 0 0 0 1"
		wg.Add(len(hosts))
		for _, host := range hosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()
	} else {
		framework.Logf("Removing network Failure")
		sshCmd := "localcli network firewall set --enabled false;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 1 --ruleset-id cmmds;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 1 --ruleset-id rdt;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 1 --ruleset-id fdm;"

		wg.Add(len(hosts))
		for _, host := range hosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()
		sshCmd = "vsish -e set /vmkModules/esxfw/globaloptions 1 1 0 1 1"
		wg.Add(len(hosts))
		for _, host := range hosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()
	}
}

// deletePVCInParallel deletes PVC in a given namespace in parallel
func deletePvcInParallel(ctx context.Context, client clientset.Interface, pvclaims []*v1.PersistentVolumeClaim,
	namespace string, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for _, pvclaim := range pvclaims {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// createPodsInParallel creates Pods in a given namespace in parallel
func createPodsInParallel(client clientset.Interface, namespace string, pvclaims []*v1.PersistentVolumeClaim,
	ctx context.Context, lock *sync.Mutex, ch chan *v1.Pod, wg *sync.WaitGroup, volumeOpsScale int) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	var pod *v1.Pod

	for i := 0; i < len(pvclaims); i++ {
		if rwxAccessMode {
			pod = fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[i]}, api.LevelBaseline, "")

		} else {
			pod = fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[i]}, api.LevelBaseline, execCommand)

		}
		pod.Spec.Containers[0].Image = busyBoxImageOnGcr
		pod, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("pod name : %s", pod.Name)
		lock.Lock()
		ch <- pod
		lock.Unlock()
	}

}

// updatePvcLabelsInParallel updates the labels of pvc in a namespace in parallel
func updatePvcLabelsInParallel(ctx context.Context, client clientset.Interface, namespace string,
	labels map[string]string, pvclaims []*v1.PersistentVolumeClaim, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for _, pvc := range pvclaims {
		framework.Logf("Updating labels %+v for pvc %s in namespace %s",
			labels, pvc.Name, namespace)
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvc.Labels = labels
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"Error on updating pvc labels is: %v", err)

	}
}

// updatePvLabelsInParallel updates the labels of pv in parallel
func updatePvLabelsInParallel(ctx context.Context, client clientset.Interface, namespace string,
	labels map[string]string, persistentVolumes []*v1.PersistentVolume, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for _, pv := range persistentVolumes {
		framework.Logf("Updating labels %+v for pv %s in namespace %s",
			labels, pv.Name, namespace)
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv.Labels = labels
		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"Error on updating pv labels is: %v", err)

	}
}

// getMasterIpOnSite returns IP address of master node on a particular site. This function has to be
// only used in deployments where k8s masters are spread across sites
func getMasterIpOnSite(ctx context.Context, client clientset.Interface, primarySite bool) (string, error) {
	siteEsxMap := make(map[string]bool)
	masterIpOnSite := ""

	siteHosts := fds.secondarySiteHosts
	if primarySite {
		siteHosts = fds.primarySiteHosts
	}

	for _, x := range siteHosts {
		siteEsxMap[x] = true
	}

	allMasterIps := getK8sMasterIPs(ctx, client)
	// Assuming atleast one master is on that site
	waitErr := wait.PollUntilContextTimeout(ctx, healthStatusPollInterval, pollTimeout*2,
		true, func(ctx context.Context) (bool, error) {
			for _, masterIp := range allMasterIps {
				govcCmd := govcLoginCmd()
				govcCmd += fmt.Sprintf("govc vm.info --vm.ip=%s;", masterIp)

				result, err := exec.Command("/bin/bash", "-c", govcCmd).Output()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("res is: %v", result)
				hostIp := strings.Split(string(result), "Host:")
				host := strings.TrimSpace(hostIp[1])
				if siteEsxMap[host] {
					masterIpOnSite = masterIp
					break
				}
			}
			if masterIpOnSite != "" {
				return true, nil
			}
			return false, nil
		})
	framework.Logf("Master IP on site : %s", masterIpOnSite)
	if waitErr != nil {
		if wait.Interrupted(waitErr) {
			return "", fmt.Errorf("couldn't find a master running on site")
		}
		return "", waitErr
	}
	return masterIpOnSite, nil
}

// changeLeaderOfContainerToComeUpOnMaster ensures that the leader of a container comes up on
// a specific master node on that site
func changeLeaderOfContainerToComeUpOnMaster(ctx context.Context, client clientset.Interface,
	sshClientConfig *ssh.ClientConfig, csiContainerName string, primarySite bool) error {
	// fetching k8s version
	v, err := client.Discovery().ServerVersion()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	k8sVersion := v.Major + "." + v.Minor

	// Fetch the IP address of master node on that site
	masterIpOnSite, err := getMasterIpOnSite(ctx, client, primarySite)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Get the master Ip where leader of csi container is running
	allMasterIps := getK8sMasterIPs(ctx, client)
	// Remove master ip which is on that site from list of master ip of all nodes
	for i, v := range allMasterIps {
		if v == masterIpOnSite {
			allMasterIps = append(allMasterIps[:i], allMasterIps[i+1:]...)
			break
		}
	}

	leaderFoundOnsite := false
	waitErr := wait.PollUntilContextTimeout(ctx, healthStatusPollInterval, pollTimeout*6, true,
		func(ctx context.Context) (bool, error) {
			// Check if leader of csi container comes up on master node of secondary site
			_, masterIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx, client, sshClientConfig,
				csiContainerName)
			framework.Logf("%s container leader is on a master node with IP %s ", csiContainerName, masterIp)
			if err != nil {
				return false, err
			}
			csipods, err := client.CoreV1().Pods(csiSystemNamespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer ginkgo.GinkgoRecover()

			// Pause and kill container of csi container on other master nodes
			if masterIp == masterIpOnSite {
				leaderFoundOnsite = true
				err = fpod.WaitForPodsRunningReady(ctx, client, csiSystemNamespace, len(csipods.Items),
					time.Duration(pollTimeout))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer ginkgo.GinkgoRecover()
				if err != nil {
					if strings.Contains(err.Error(), "are NOT in RUNNING and READY state") {
						framework.Logf("Rechecking CSI pod status to verify it is in running state")
						ignoreLabels := make(map[string]string)
						list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, csiSystemNamespace, ignoreLabels)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						for i := 0; i < len(list_of_pods); i++ {
							_ = fpod.WaitTimeoutForPodRunningInNamespace(ctx, client, list_of_pods[i].Name,
								csiSystemNamespace, pollTimeout*2)
						}
					} else {
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
				framework.Logf("Leader of %s found on site", csiContainerName)
				return true, nil
			}

			var wg sync.WaitGroup
			wg.Add(len(allMasterIps))
			for _, masterIp := range allMasterIps {
				go invokeDockerPauseNKillOnContainerInParallel(sshClientConfig, masterIp,
					csiContainerName, k8sVersion, &wg)
			}
			wg.Wait()

			return false, nil
		})

	if !leaderFoundOnsite {
		return fmt.Errorf("couldn't get %s leader on %s", csiContainerName, masterIpOnSite)
	}
	return waitErr
}

// invokeDockerPauseNKillOnContainerInParallel invokes docker pause and kill command on
// the particular CSI container on the master node in parallel
func invokeDockerPauseNKillOnContainerInParallel(sshClientConfig *ssh.ClientConfig, k8sMasterIp string,
	csiContainerName string, k8sVersion string, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	err := execDockerPauseNKillOnContainer(sshClientConfig, k8sMasterIp, csiContainerName, k8sVersion)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// toggleWitnessPowerState causes witness host to be powered on or off
func toggleWitnessPowerState(ctx context.Context, witnessHostDown bool) {
	witnessHost := []string{fds.witness}
	if witnessHostDown {
		framework.Logf("hosts to power off: %v", witnessHost)
		powerOffHostParallel(ctx, witnessHost)
		fds.witnessDown = fds.witness
	} else {
		framework.Logf("hosts to power on: %v", witnessHost)
		powerOnHostParallel(witnessHost)
		fds.witnessDown = ""
	}

}

func checkVmStorageCompliance(storagePolicy string) bool {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := govcLoginCmd()
	cmd += fmt.Sprintf("govc storage.policy.info -c -s %s;", storagePolicy)
	framework.Logf("Running command: %s", cmd)
	result, err := exec.Command("/bin/bash", "-c", cmd).Output()
	framework.Logf("res is: %v", result)
	return strings.Contains(err.Error(), "object references is empty")
}

// createStsDeployment creates statfulset and deployment in a namespace and returns
// statefulset, deployment and volumes of statfulset created
func createStsDeployment(ctx context.Context, client clientset.Interface, namespace string,
	sc *storagev1.StorageClass, isDeploymentRequired bool, modifyStsSpec bool,
	stsReplica int32, stsName string, depReplicaCount int32,
	accessMode v1.PersistentVolumeAccessMode) (*appsv1.StatefulSet, *appsv1.Deployment, []string) {
	var pvclaims []*v1.PersistentVolumeClaim
	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}
	statefulset := GetStatefulSetFromManifest(namespace)
	framework.Logf("Creating statefulset")
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
		Spec.StorageClassName = &sc.Name
	statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
		accessMode
	if modifyStsSpec {
		statefulset.Name = stsName
		statefulset.Spec.Template.Labels["app"] = statefulset.Name
		statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
		*(statefulset.Spec.Replicas) = stsReplica
	}
	CreateStatefulSet(namespace, statefulset, client)
	replicas := *(statefulset.Spec.Replicas)
	// Waiting for pods status to be Ready
	fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
	gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
		"Unable to get list of Pods from the Statefulset: %v", statefulset.Name)
	gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset %s, %v, should match with number of required replicas %v",
		statefulset.Name, ssPodsBeforeScaleDown.Size(), replicas)

	// Get the list of Volumes attached to Pods before scale down
	var volumesBeforeScaleDown []string
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
				// Verify the attached volume match the one in CNS cache
				if !multivc {
					if guestCluster {
						volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
						svcPVCName := pv.Spec.CSI.VolumeHandle
						pvcName := volumespec.PersistentVolumeClaim.ClaimName

						pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							pvcName, metav1.GetOptions{})
						gomega.Expect(pvclaim).NotTo(gomega.BeNil())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, volHandle, svcPVCName, pvclaim,
							pv, &sspod)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}

				} else {
					err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}
	}
	if isDeploymentRequired {
		framework.Logf("Creating PVC")
		pvclaim, err := createPVC(ctx, client, namespace, nil, diskSize, sc, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		framework.Logf("Creating Deployment")
		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		deployment, err := createDeployment(
			ctx, client, depReplicaCount, labelsMap, nil, namespace, pvclaims, "", false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err := fdep.GetPodsForDeployment(ctx, client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		return statefulset, deployment, volumesBeforeScaleDown
	}

	return statefulset, nil, volumesBeforeScaleDown
}

// volumeLifecycleActions creates pvc and pod and waits for them to be in healthy state and then deletes them
func volumeLifecycleActions(ctx context.Context, client clientset.Interface, namespace string,
	sc *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode) {

	var vmUUID string
	var exists bool

	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}

	if rwxAccessMode {
		pvc1, err := createPVC(ctx, client, namespace, nil, "", sc, accessMode)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(time.Duration(80) * time.Second)

		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx,
			client, []*v1.PersistentVolumeClaim{pvc1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle

		verifyVolPropertiesFromCnsQueryResults(e2eVSphere, volHandle)

		//Create Pod1
		ginkgo.By(fmt.Sprintf("create pod with pvc: %s", pvc1.Name))
		pod1, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Create file1.txt on Pod1
		ginkgo.By("Create file1.txt on Pod1")
		err = e2eoutput.CreateEmptyFileOnPod(namespace, pod1.Name, filePath1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		//Write data on file1.txt on Pod1
		data := "This file file1 is written by Pod1"
		ginkgo.By("Write on file1.txt from Pod1")
		writeDataOnFileFromPod(namespace, pod1.Name, filePath1, data)

		ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod1.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvs[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		framework.Logf("Creating RWO PVC")
		pvc1, err := createPVC(ctx, client, namespace, nil, diskSize, sc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx,
			client, []*v1.PersistentVolumeClaim{pvc1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle

		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(pvs[0].Spec.CSI.VolumeHandle)
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		pod1, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc1}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if vanillaCluster {
			vmUUID = getNodeUUID(ctx, client, pod1.Spec.NodeName)
			framework.Logf("VMUUID : %s", vmUUID)
		} else if guestCluster {
			vmUUID, err = getVMUUIDFromNodeName(pod1.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("VMUUID : %s", vmUUID)
		} else if supervisorCluster {
			annotations := pod1.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			framework.Logf("VMUUID : %s", vmUUID)
		}
		framework.Logf("VMUUID : %s", vmUUID)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
			"Volume is not attached to the node volHandle: %s, vmUUID: %s", volHandle, vmUUID)

		framework.Logf("Verify the volume is accessible")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod1.Name,
			[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if guestCluster && rwxAccessMode {
			ginkgo.By("Verifying whether the CnsFileAccessConfig CRD is created or not for Pod1")
			verifyCNSFileAccessConfigCRDInSupervisor(ctx, pod1.Spec.NodeName+"-"+volHandle,
				crdCNSFileAccessConfig, crdVersion, crdGroup, true)
			deletePodAndWaitForVolsToDetach(ctx, client, pod1)
		}

		deletePodAndWaitForVolsToDetach(ctx, client, pod1)

		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc1.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeHandle := pvs[0].Spec.CSI.VolumeHandle
		err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pvs[0].Name, poll,
			pollTimeout)
		errMsg := "The object or item referred to could not be found"
		if err != nil && checkForEventWithMessage(client, "", pvs[0].Name, errMsg) {
			framework.Logf("Persistent Volume %v still not deleted with err %v", pvs[0].Name, errMsg)
			// Orphan volumes may be left over here, hence logging those PVs and ignoring the error for now.
			_ = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			framework.Logf("Volume %v still not deleted from CNS with err %v", pvs[0].Name, errMsg)
		} else {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

}

// scaleDownStsAndVerifyPodMetadata scales down replica of a statefulset if required
// and verifies count of sts replica and  if its vSphere volumes match those in CNS cache
func scaleDownStsAndVerifyPodMetadata(ctx context.Context, client clientset.Interface,
	namespace string, statefulset *appsv1.StatefulSet, ssPodsBeforeScaleDown *v1.PodList,
	replicas int32, isScaleDownRequired bool, verifyCnsVolumes bool) {
	if isScaleDownRequired {
		framework.Logf("Scaling down statefulset: %v to number of Replica: %v",
			statefulset.Name, replicas)
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
	}
	fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
	ssPodsAfterScaleDown, err := fss.GetPodList(ctx, client, statefulset)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset %s, %v, should match with number of replicas %v",
		statefulset.Name, ssPodsAfterScaleDown.Size(), replicas,
	)

	// After scale down, verify vSphere volumes are detached from deleted pods
	if verifyCnsVolumes {
		framework.Logf("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if vanillaCluster {
							isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
								client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
								fmt.Sprintf("Volume %q is not detached from the node %q",
									pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						} else if supervisorCluster {
							annotations := sspod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(),
								fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

							framework.Logf("Verify volume: %s is detached from PodVM with vmUUID: %s",
								pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
							gomega.Expect(err).To(gomega.HaveOccurred(),
								fmt.Sprintf(
									"PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
									vmUUID, sspod.Spec.NodeName))
						} else {

							ginkgo.By("Verify volume is detached from the node")
							isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
								pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
							gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
								fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))

						}
					}
				}
			}
		}

		// After scale down, verify the attached volumes match those in CNS Cache
		for _, sspod := range ssPodsAfterScaleDown.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					if guestCluster {
						volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
						svcPVCName := pv.Spec.CSI.VolumeHandle
						pvcName := volumespec.PersistentVolumeClaim.ClaimName

						pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							pvcName, metav1.GetOptions{})
						gomega.Expect(pvclaim).NotTo(gomega.BeNil())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, volHandle, svcPVCName, pvclaim,
							pv, &sspod)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {

						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}
	}
}

// scaleUpStsAndVerifyPodMetadata scales up replica of a statefulset if required
// and verifies count of sts replica and if its vSphere volumes are attached to node VMs
func scaleUpStsAndVerifyPodMetadata(ctx context.Context, client clientset.Interface,
	namespace string, statefulset *appsv1.StatefulSet,
	replicas int32, isScaleUpRequired bool, verifyCnsVolumes bool) {
	if isScaleUpRequired {
		framework.Logf("Scaling up statefulset: %v to number of Replica: %v",
			statefulset.Name, replicas)
		_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
	}

	fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
	fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
	ssPodsAfterScaleUp, err := fss.GetPodList(ctx, client, statefulset)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset %s, %v, should match with number of replicas %v",
		statefulset.Name, ssPodsAfterScaleUp.Size(), replicas,
	)

	if verifyCnsVolumes {
		// After scale up, verify all vSphere volumes are attached to node VMs.
		framework.Logf("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			err := fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range pod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					framework.Logf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
					var vmUUID, volHandle string
					var exists bool
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					volHandle = pv.Spec.CSI.VolumeHandle
					if vanillaCluster {
						vmUUID = getNodeUUID(ctx, client, sspod.Spec.NodeName)
					} else if supervisorCluster {
						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(
							gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("VMUUID : %s", vmUUID)
						volHandle = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
					}
					if !rwxAccessMode {
						isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
					}
					framework.Logf("After scale up, verify the attached volumes match those in CNS Cache")
					if guestCluster {
						volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
						gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
						svcPVCName := pv.Spec.CSI.VolumeHandle
						pvcName := volumespec.PersistentVolumeClaim.ClaimName

						pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx,
							pvcName, metav1.GetOptions{})
						gomega.Expect(pvclaim).NotTo(gomega.BeNil())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = waitAndVerifyCnsVolumeMetadata4GCVol(ctx, volHandle, svcPVCName, pvclaim,
							pv, &sspod)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}
	}
}

// deleteCsiPodInParallel deletes csi pod present in csi namespace in parallel
func deleteCsiPodInParallel(ctx context.Context, client clientset.Interface, pod *v1.Pod,
	wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	framework.Logf("Deleting the pod: %s", pod.Name)
	err := fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// deleteCsiControllerPodOnOtherMasters deletes the CSI Controller Pod
// on other master nodes which are not present on that site.
func deleteCsiControllerPodOnOtherMasters(ctx context.Context, client clientset.Interface,
	csiPodOnSite string) {
	ignoreLabels := make(map[string]string)
	csiPods, err := fpod.GetPodsInNamespace(ctx, client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Remove csi pod which is running on that site from list of all csi Pods
	var otherCsiControllerPods []*v1.Pod
	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, vSphereCSIControllerPodNamePrefix) &&
			csiPod.Name != csiPodOnSite {
			otherCsiControllerPods = append(otherCsiControllerPods, csiPod)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(otherCsiControllerPods))
	for _, csiPod := range otherCsiControllerPods {
		go deleteCsiPodInParallel(ctx, client, csiPod, &wg)
	}
	wg.Wait()
}

// hostFailure causes a host in either site to be powered on or off
func hostFailure(ctx context.Context, esxHost string, hostDown bool) {
	host := []string{esxHost}
	if hostDown {
		framework.Logf("hosts to power off: %v", host)
		powerOffHostParallel(ctx, host)
	} else {
		framework.Logf("hosts to power on: %v", host)
		powerOnHostParallel(host)
	}
}

// scaleStsReplicaInParallel scales statefulset's replica up/down in parallel
func scaleStsReplicaInParallel(ctx context.Context, client clientset.Interface, stsList []*appsv1.StatefulSet,
	regex string, replicas int32, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for _, statefulset := range stsList {
		if strings.Contains(statefulset.Name, regex) {
			fss.UpdateReplicas(ctx, client, statefulset, replicas)
		}
	}
}

// deletePvInParallel deletes PVs in parallel from k8s cluster
func deletePvInParallel(ctx context.Context, client clientset.Interface, persistentVolumes []*v1.PersistentVolume,
	wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for _, pv := range persistentVolumes {
		framework.Logf("Deleting pv %s", pv.Name)
		err := fpv.DeletePersistentVolume(ctx, client, pv.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// createStaticPvAndPvcInParallel creates static pv from given fcdId and pvc in a particular namespace
// in parallel
func createStaticPvAndPvcInParallel(client clientset.Interface, ctx context.Context, fcdIDs []string,
	ch chan *v1.PersistentVolumeClaim, namespace string, wg *sync.WaitGroup,
	volumeOpsScale int) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	staticPVLabels := make(map[string]string)
	for i := 0; i < volumeOpsScale; i++ {
		// Creating label for PV.
		// PVC will use this label as Selector to find PV.
		staticPVLabels["fcd-id"] = fcdIDs[i]
		framework.Logf("Creating the PV from fcd ID: %s", fcdIDs[i])
		pv := getPersistentVolumeSpec(fcdIDs[i], v1.PersistentVolumeReclaimRetain, staticPVLabels, ext4FSType)
		pv, err := client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Creating the PVC from PV: %s", pv.Name)
		pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
		pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ch <- pvc

	}
}

// triggerFullSyncInParallel triggers full sync on demand in parallel. Here, we are
// ignoring full sync failures due to site failover/failback. Hence, we are not
// using triggerFullSync() here
func triggerFullSyncInParallel(ctx context.Context,
	cnsOperatorClient client.Client, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	err := waitForFullSyncToFinish(ctx, cnsOperatorClient)
	if err != nil {
		framework.Logf("Full sync did not finish in given time, ignoring this error: %v", err)
	}

	crd := getTriggerFullSyncCrd(ctx, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)
	updateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd)
	err = waitForFullSyncToFinish(ctx, cnsOperatorClient)
	if err != nil {
		framework.Logf("Full sync did not finish in given time, ignoring this error: %v", err)
	}
	crd = getTriggerFullSyncCrd(ctx, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)
	updateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd)
	err = waitForFullSyncToFinish(ctx, cnsOperatorClient)
	if err != nil {
		framework.Logf("Full sync did not finish in given time, ignoring this error: %v", err)
	}
}

// getTriggerFullSyncCrd fetches full sync crd from the list of crds in k8s cluster
func getTriggerFullSyncCrd(ctx context.Context,
	cnsOperatorClient client.Client) *triggercsifullsyncv1alpha1.TriggerCsiFullSync {
	fullSyncCrd := &triggercsifullsyncv1alpha1.TriggerCsiFullSync{}
	err := cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: crdtriggercsifullsyncsName}, fullSyncCrd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(fullSyncCrd).NotTo(gomega.BeNil(), "couldn't find full sync crd: %s", crdtriggercsifullsyncsName)
	return fullSyncCrd
}

// updateTriggerFullSyncCrd triggers full sync by updating TriggerSyncID
// value to  LastTriggerSyncID +1 in full sync crd
func updateTriggerFullSyncCrd(ctx context.Context, cnsOperatorClient client.Client,
	crd triggercsifullsyncv1alpha1.TriggerCsiFullSync) {
	framework.Logf("instance is %v before update", crd)
	lastSyncId := crd.Status.LastTriggerSyncID
	triggerSyncID := lastSyncId + 1
	crd.Spec.TriggerSyncID = triggerSyncID

	err := cnsOperatorClient.Update(ctx, &crd)
	framework.Logf("Error is %v", err)

	if apierrors.IsConflict(err) {
		latest_crd := getTriggerFullSyncCrd(ctx, cnsOperatorClient)
		framework.Logf("INFO: full sync crd details: %v", latest_crd)
		lastSyncId := latest_crd.Status.LastTriggerSyncID
		triggerSyncID := lastSyncId + 1
		latest_crd.Spec.TriggerSyncID = triggerSyncID
		err = cnsOperatorClient.Update(ctx, latest_crd)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("instance is %v after update", crd)
}

// waitForFullSyncToFinish waits for a given full sync to finish by checking
// InProgress field in trigger full sync crd
func waitForFullSyncToFinish(ctx context.Context,
	cnsOperatorClient client.Client) error {
	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeoutShort, true,
		func(ctx context.Context) (bool, error) {
			crd := getTriggerFullSyncCrd(ctx, cnsOperatorClient)
			framework.Logf("crd is: %v", crd)
			if !crd.Status.InProgress {
				return true, nil
			}
			if crd.Status.Error != "" {
				return false, fmt.Errorf("full sync failed with error: %s", crd.Status.Error)
			}
			return false, nil
		})
	return waitErr
}

// checkForEventWithMessage fetches events list of the given object name and checkes for
// specified message and returns true if expected error message found
func checkForEventWithMessage(client clientset.Interface, namespace string,
	name string, expectedMsg string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventFound := false
	framework.Logf("Checking for error in events related to %q", name)
	eventList, _ := client.CoreV1().Events(namespace).List(ctx,
		metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", name)})
	for _, item := range eventList.Items {
		framework.Logf("message: %v", item.Message)
		if strings.Contains(item.Message, expectedMsg) {
			framework.Logf("Expected event found. EventList Reason: "+
				"%q"+" EventList item: %q", item.Reason, item.Message)
			eventFound = true
			break
		}
	}
	return eventFound
}

// psodHostsOnSite executes PSOD operation on the hosts of the given site
func psodHostsOnSite(primarySite bool, psodTimeout string) {
	hosts := fds.secondarySiteHosts
	if primarySite {
		hosts = fds.primarySiteHosts
	}

	for _, host := range hosts {
		err := psodHost(host, psodTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// psodHostsInParallel is a rapper method for psodHostsOnSite method
// which performs PSOD operation on the hosts of the given fault domain concurrently
func psodHostsInParallel(primarySite bool, psodTimeout string, wg *sync.WaitGroup) {
	defer wg.Done()
	psodHostsOnSite(primarySite, psodTimeout)
}

// createStaticPvAndPvcInGuestClusterInParallel creates PV and PVC in a guest cluster
// from a volume created in supervisor cluster concurrently
func createStaticPvAndPvcInGuestClusterInParallel(client clientset.Interface, ctx context.Context,
	namespace string, svcPVCNames []string, storageClassName string,
	ch chan *v1.PersistentVolumeClaim, lock *sync.Mutex, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for _, svcPVCName := range svcPVCNames {
		framework.Logf("Volume Handle :%s", svcPVCName)

		ginkgo.By("Creating PV in guest cluster")
		gcPV := getPersistentVolumeSpecWithStorageclass(svcPVCName,
			v1.PersistentVolumeReclaimRetain, storageClassName, nil, diskSize)
		gcPV, err := client.CoreV1().PersistentVolumes().Create(ctx, gcPV, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gcPVName := gcPV.GetName()
		time.Sleep(time.Duration(10) * time.Second)
		framework.Logf("PV name in GC : %s", gcPVName)

		ginkgo.By("Creating PVC in guest cluster")
		gcPVC := getPVCSpecWithPVandStorageClass(svcPVCName, namespace, nil, gcPVName, storageClassName, diskSize)
		gcPVC, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, gcPVC, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		lock.Lock()
		ch <- gcPVC
		lock.Unlock()
	}
}

// createDynamicSnapshotInParallel creates dynamic volumesnapshots from
// a list of PVCs in a given namespace in parallel
func createDynamicSnapshotInParallel(ctx context.Context, namespace string,
	snapc *snapclient.Clientset, pvcList []*v1.PersistentVolumeClaim, volumeSnapshotClassName string,
	ch chan *snapV1.VolumeSnapshot, lock *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	ginkgo.By("Create a volume snapshot")
	for _, pvc := range pvcList {
		snapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
			getVolumeSnapshotSpec(namespace, volumeSnapshotClassName, pvc.Name), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		lock.Lock()
		ch <- snapshot
		lock.Unlock()
		framework.Logf("Volume snapshot name is : %s", snapshot.Name)
	}
}

// makeHostLoseStorageWithOtherHosts causes or removes network fault on a particular host
func makeHostLoseStorageConnectivityWithOtherHosts(esxHosts []string, causeNetworkFailure bool) {
	var wg sync.WaitGroup
	if causeNetworkFailure {
		framework.Logf("Creating a Network Failure with other hosts")
		sshCmd := "localcli network firewall set --enabled true;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 0 --ruleset-id cmmds;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 0 --ruleset-id rdt;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 0 --ruleset-id fdm;"

		wg.Add(len(esxHosts))
		for _, host := range esxHosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()

		for i := range esxHosts {
			sshCmd = ""
			remainingHosts := append(esxHosts[:i], esxHosts[i+1:]...)
			for _, host := range remainingHosts {
				sshCmd = fmt.Sprintf("localcli network firewall ruleset allowedip add -i %s -r rdt;", host)
				sshCmd += fmt.Sprintf("localcli network firewall ruleset allowedip add -i %s -r cmmds;", host)
				sshCmd += fmt.Sprintf("localcli network firewall ruleset allowedip add -i %s -r fdm;", host)
			}
			_, err := runCommandOnESX(rootUser, esxHosts[i], sshCmd)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		sshCmd = "vsish -e set /vmkModules/esxfw/globaloptions 1 0 0 0 1"

		wg.Add(len(esxHosts))
		for _, host := range esxHosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()

	} else {
		framework.Logf("Removing network Failure with other hosts")
		sshCmd := "localcli network firewall set --enabled false;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 1 --ruleset-id cmmds;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 1 --ruleset-id rdt;"
		sshCmd += "localcli network firewall ruleset set --allowed-all 1 --ruleset-id fdm;"

		wg.Add(len(esxHosts))
		for _, host := range esxHosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()

		for i := range esxHosts {
			sshCmd = ""
			remainingHosts := append(esxHosts[:i], esxHosts[i+1:]...)
			for _, host := range remainingHosts {
				sshCmd = fmt.Sprintf("localcli network firewall ruleset allowedip remove -i %s -r rdt;", host)
				sshCmd += fmt.Sprintf("localcli network firewall ruleset allowedip remove -i %s -r cmmds;", host)
				sshCmd += fmt.Sprintf("localcli network firewall ruleset allowedip remove -i %s -r fdm;", host)
			}
			_, err := runCommandOnESX(rootUser, esxHosts[i], sshCmd)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		sshCmd = "vsish -e set /vmkModules/esxfw/globaloptions 1 1 0 1 1"

		wg.Add(len(esxHosts))
		for _, host := range esxHosts {
			go runCmdOnHostsInParallel(host, sshCmd, &wg)
		}
		wg.Wait()

	}
}
