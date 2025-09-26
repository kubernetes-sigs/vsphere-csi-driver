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

package vsan_stretch

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vsan"
	vsantypes "github.com/vmware/govmomi/vsan/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"

	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/vc"
	vsanClient "sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/vsan"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/hosts"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/nimbus"
)

const (
	filePath1  = "/mnt/volume1/file1.txt"
	filePath2  = "/mnt/volume1/file2.txt"
	accessMode = v1.ReadWriteMany
)

type FaultDomains struct {
	PrimarySiteHosts   []string
	SecondarySiteHosts []string
	Witness            string
	HostsDown          []string `default:"[]"`
	HostsPartitioned   []string `default:"[]"`
	WitnessDown        string
}

var Fds FaultDomains

// initialiseFdsVar initialise fds variable
func InitialiseFdsVar(ctx context.Context, vs *config.E2eTestConfig) {
	fdMap := CreateFaultDomainMap(ctx, vs)
	hostsWithoutFD := []string{}
	for host, site := range fdMap {
		if strings.Contains(site, "rimary") {
			Fds.PrimarySiteHosts = append(Fds.PrimarySiteHosts, host)
		} else if strings.Contains(site, "econdary") {
			Fds.SecondarySiteHosts = append(Fds.SecondarySiteHosts, host)
		} else {
			hostsWithoutFD = append(hostsWithoutFD, host)
		}
	}

	// assuming we don't have hosts which are not part of the vsan stretched cluster in the testbed here
	gomega.Expect(len(hostsWithoutFD) == 1).To(gomega.BeTrue())
	Fds.Witness = hostsWithoutFD[0]

}

// siteFailureInParallel causes site Failure in multiple hosts of the site in parallel
func SiteFailureInParallel(ctx context.Context, vs *config.E2eTestConfig, primarySite bool, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	SiteFailover(ctx, vs, primarySite)
}

// siteFailover causes a site failover by powering off hosts of the given site
func SiteFailover(ctx context.Context, vs *config.E2eTestConfig, primarySite bool) {
	hostsToPowerOff := Fds.SecondarySiteHosts
	if primarySite {
		hostsToPowerOff = Fds.PrimarySiteHosts
	}
	framework.Logf("hosts to power off: %v", hostsToPowerOff)
	PowerOffHostParallel(ctx, vs, hostsToPowerOff)
}

// powerOffHostParallel powers off given hosts
func PowerOffHostParallel(ctx context.Context, vs *config.E2eTestConfig, hostsToPowerOff []string) {
	hostlist := ""
	for _, host := range hostsToPowerOff {
		for _, esxHost := range nimbus.Tbinfo.EsxHosts {
			if esxHost["ip"] == host {
				hostlist += esxHost["vmName"] + " "
			}
		}
		Fds.HostsDown = append(Fds.HostsDown, host)
	}

	err := nimbus.VMPowerMgmt(nimbus.Tbinfo.User, nimbus.Tbinfo.Location, nimbus.Tbinfo.Podname, hostlist, false)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, host := range hostsToPowerOff {
		err = WaitForHostToBeDown(vs, ctx, host)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// siteRestore restores a site by powering on hosts of the given site
func SiteRestore(vs *config.E2eTestConfig, primarySite bool) {
	hostsToPowerOn := Fds.SecondarySiteHosts
	if primarySite {
		hostsToPowerOn = Fds.PrimarySiteHosts
	}
	framework.Logf("hosts to power on: %v", hostsToPowerOn)
	PowerOnHostParallel(vs, hostsToPowerOn)
}

// powerOnHostParallel powers on given hosts
func PowerOnHostParallel(vs *config.E2eTestConfig, hostsToPowerOn []string) {
	hostlist := ""
	for _, host := range hostsToPowerOn {
		for _, esxHost := range nimbus.Tbinfo.EsxHosts {
			if esxHost["ip"] == host {
				hostlist += esxHost["vmName"] + " "
			}
		}
		Fds.HostsDown = append(Fds.HostsDown, host)
	}
	err := nimbus.VMPowerMgmt(nimbus.Tbinfo.User, nimbus.Tbinfo.Location, nimbus.Tbinfo.Podname, hostlist, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, host := range hostsToPowerOn {
		err = hosts.WaitForHostToBeUp(vs, host, time.Minute*40)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// createFaultDomainMap returns the host to fault domain mapping
func CreateFaultDomainMap(ctx context.Context, vs *config.E2eTestConfig) map[string]string {
	fdMap := make(map[string]string)
	c := vc.NewClient(ctx, vs.TestInput.Global.VCenterHostname,
		vs.TestInput.Global.VCenterPort, vs.TestInput.Global.User, vs.TestInput.Global.Password)

	datacenter := strings.Split(vs.TestInput.Global.Datacenters, ",")[0]

	vsanHealthClient, err := vsanClient.NewVsanHealthSvcClient(ctx, c.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vsanClient, err := vsan.NewClient(ctx, vsanHealthClient.Vim25Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	finder := find.NewFinder(vsanHealthClient.Vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	hosts, err := finder.HostSystemList(ctx, "*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if !vs.TestInput.ClusterFlavor.VanillaCluster {
		hostsInVsanStretchCluster := []*object.HostSystem{}
		for _, host := range hosts {
			hostInfo := host.Common.InventoryPath
			hostIpInfo := strings.Split(hostInfo, "/")
			hostCluster := hostIpInfo[len(hostIpInfo)-2]
			if !strings.Contains(hostCluster, "EdgeMgmtCluster") {
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
		waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout*2, true,
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
func WaitForHostToBeDown(vs *config.E2eTestConfig, ctx context.Context, ip string) error {
	framework.Logf("checking host status of %s", ip)
	gomega.Expect(ip).NotTo(gomega.BeNil())
	gomega.Expect(ip).NotTo(gomega.BeEmpty())
	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, ip)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll*2, constants.PollTimeoutShort*2, true,
		func(ctx context.Context) (bool, error) {
			_, err := net.DialTimeout("tcp", addr, constants.Poll)
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
func WaitForAllNodes2BeReady(ctx context.Context, c clientset.Interface, timeout ...time.Duration) error {
	var pollTime time.Duration
	if len(timeout) > 0 {
		pollTime = timeout[0]
	} else {
		if os.Getenv("K8S_NODES_UP_WAIT_TIME") != "" {
			k8sNodeWaitTime, err := strconv.Atoi(os.Getenv(constants.EnvK8sNodesUpWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pollTime = time.Duration(k8sNodeWaitTime) * time.Minute
		} else {
			pollTime = time.Duration(constants.DefaultK8sNodesUpWaitTime) * time.Minute
		}
	}
	framework.Logf("Waiting up to %v for all nodes to be ready", pollTime)

	var notReady []v1.Node
	err := wait.PollUntilContextTimeout(ctx, constants.Poll, pollTime, true,
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
func Wait4AllK8sNodesToBeUp(vs *config.E2eTestConfig, k8sNodes *v1.NodeList) {
	var nodeIp string
	for _, node := range k8sNodes.Items {
		addrs := node.Status.Addresses
		for _, addr := range addrs {
			if addr.Type == v1.NodeInternalIP && (net.ParseIP(addr.Address)).To4() != nil {
				nodeIp = addr.Address
			}
		}
		err := hosts.WaitForHostToBeUp(vs, nodeIp, time.Minute*40)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}
