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
	"strings"
	"sync"
	"time"

	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	vsan "github.com/vmware/govmomi/vsan"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

type FaultDomains struct {
	primarySiteHosts   []string
	secondarySiteHosts []string
	witness            string
	hostsDown          []string `default:"[]"`
	hostsPartitioned   []string `default:"[]"`
}

var fds FaultDomains

//initialiseFdsVar initialise fds variable
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
func siteFailureInParallel(primarySite bool, wg *sync.WaitGroup) {
	defer wg.Done()
	siteFailover(primarySite)
}

//siteFailover causes a site failover by powering off hosts of the given site
func siteFailover(primarySite bool) {
	hostsToPowerOff := fds.secondarySiteHosts
	if primarySite {
		hostsToPowerOff = fds.primarySiteHosts
	}
	framework.Logf("hosts to power off: %v", hostsToPowerOff)
	powerOffHostParallel(hostsToPowerOff)
}

//powerOffHostParallel powers off given hosts
func powerOffHostParallel(hostsToPowerOff []string) {
	hostlist := ""
	for _, host := range hostsToPowerOff {
		for _, esxHost := range tbinfo.esxHosts {
			if esxHost["ip"] == host {
				hostlist += esxHost["vmName"] + " "
			}
		}
		fds.hostsDown = append(fds.hostsDown, host)
	}

	err := vMPowerMgmt(tbinfo.user, tbinfo.location, hostlist, false)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, host := range hostsToPowerOff {
		err = waitForHostToBeDown(host)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

//siteRestore restores a site by powering on hosts of the given site
func siteRestore(primarySite bool) {
	hostsToPowerOn := fds.secondarySiteHosts
	if primarySite {
		hostsToPowerOn = fds.primarySiteHosts
	}
	framework.Logf("hosts to power on: %v", hostsToPowerOn)
	powerOnHostParallel(hostsToPowerOn)
}

//powerOnHostParallel powers on given hosts
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
	err := vMPowerMgmt(tbinfo.user, tbinfo.location, hostlist, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, host := range hostsToPowerOn {
		err = waitForHostToBeUp(host)
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
	for _, host := range hosts {
		vsanSystem, _ := host.ConfigManager().VsanSystem(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		hostConfig, err := vsanClient.VsanHostGetConfig(ctx, vsanSystem.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		fdMap[host.Name()] = ""
		if hostConfig.FaultDomainInfo != nil {
			fdMap[host.Name()] = hostConfig.FaultDomainInfo.Name
			framework.Logf("host: %s, site: %s", host.Name(), hostConfig.FaultDomainInfo.Name)
		}
	}

	return fdMap
}

//waitForHostToBeDown wait for host to be down
func waitForHostToBeDown(ip string) error {
	framework.Logf("checking host status of %s", ip)
	gomega.Expect(ip).NotTo(gomega.BeNil())
	gomega.Expect(ip).NotTo(gomega.BeEmpty())
	waitErr := wait.Poll(poll*2, pollTimeoutShort*2, func() (bool, error) {
		_, err := net.DialTimeout("tcp", ip+":22", poll)
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
func waitForAllNodes2BeReady(ctx context.Context, c clientset.Interface, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for all nodes to be ready", timeout)

	var notReady []v1.Node
	err := wait.PollImmediate(poll, timeout, func() (bool, error) {
		notReady = nil
		nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		framework.Logf("error is %v", err)

		if err != nil && !strings.Contains(err.Error(), "has prevented the request") &&
			!strings.Contains(err.Error(), "TLS handshake timeout") {
			return false, err
		}
		for _, node := range nodes.Items {
			if !fnodes.IsConditionSetAsExpected(&node, v1.NodeReady, true) {
				notReady = append(notReady, node)
			}
		}
		return len(notReady) == 0 && err == nil, nil
	})
	if len(notReady) > 0 {
		return fmt.Errorf("not ready nodes: %v", notReady)
	}

	return err
}

//wait4AllK8sNodesToBeUp wait for all k8s nodes to be reachable
func wait4AllK8sNodesToBeUp(
	ctx context.Context, client clientset.Interface, k8sNodes *v1.NodeList) {
	var nodeIp string
	for _, node := range k8sNodes.Items {
		addrs := node.Status.Addresses
		for _, addr := range addrs {
			if addr.Type == v1.NodeInternalIP && (net.ParseIP(addr.Address)).To4() != nil {
				nodeIp = addr.Address
			}
		}
		err := waitForHostToBeUp(nodeIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// deletePodsInParallel deletes pods in a given namespace in parallel
func deletePodsInParallel(client clientset.Interface, namespace string, pods []*v1.Pod, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, pod := range pods {
		fpod.DeletePodOrFail(client, namespace, pod.Name)
	}
}

// createPvcInParallel creates number of PVC in a given namespace in parallel
func createPvcInParallel(client clientset.Interface, namespace string, diskSize string, sc *storagev1.StorageClass,
	ch chan *v1.PersistentVolumeClaim, lock *sync.Mutex, wg *sync.WaitGroup, volumeOpsScale int) {
	defer wg.Done()
	for i := 0; i < volumeOpsScale; i++ {
		pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
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
func waitForPodsToBeInErrorOrRunning(c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	waitErr := wait.PollImmediate(poll, timeout, func() (bool, error) {
		pod, err := c.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		framework.Logf("Pod is in phase: %v", pod.Status.Phase)
		switch pod.Status.Phase {
		// v1.PodSucceeded is for pods in ExitCode:0 state.
		// Standalone pods are in ExitCode:0 or Running state after site failure.
		case v1.PodRunning, v1.PodSucceeded:
			framework.Logf("Pod %v is in state %v", podName, pod.Status.Phase)
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// runCmdOnHostsInParallel runs command on multiple ESX in parallel
func runCmdOnHostsInParallel(hostIP string, sshCmd string, wg *sync.WaitGroup) {
	defer wg.Done()
	op, err := runCommandOnESX("root", hostIP, sshCmd)
	framework.Logf(op)
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
func deletePvcInParallel(client clientset.Interface, pvclaims []*v1.PersistentVolumeClaim,
	namespace string, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, pvclaim := range pvclaims {
		err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// createPodsInParallel creates Pods in a given namespace in parallel
func createPodsInParallel(client clientset.Interface, namespace string, pvclaims []*v1.PersistentVolumeClaim,
	ctx context.Context, lock *sync.Mutex, ch chan *v1.Pod, wg *sync.WaitGroup, volumeOpsScale int) {
	defer wg.Done()

	for i := 0; i < volumeOpsScale; i++ {
		pod := fpod.MakePod(namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[i]}, false, execCommand)
		pod.Spec.Containers[0].Image = busyBoxImageOnGcr
		pod, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		lock.Lock()
		ch <- pod
		lock.Unlock()
	}
}
