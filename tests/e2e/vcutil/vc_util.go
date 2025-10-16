/*
Copyright 2025 The Kubernetes Authors.

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
package vcutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/cns"
	cnsmethods "github.com/vmware/govmomi/cns/methods"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanpackage "github.com/vmware/govmomi/vsan"
	vsanmethods "github.com/vmware/govmomi/vsan/methods"
	vsantypes "github.com/vmware/govmomi/vsan/types"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"

	cnsclient "sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/cns"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/vc"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/vsan"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/connections"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/nimbus"
)

// This function will check the status of hosts and also wait for
// constants.PollTimeout minutes to make sure host is reachable.
func WaitForHostToBeUp(ip string, vs *config.E2eTestConfig, pollInfo ...time.Duration) error {
	framework.Logf("checking host status of %v", ip)
	pollTimeOut := constants.HealthStatusPollTimeout
	pollInterval := 30 * time.Second
	// var to store host reachability count
	hostReachableCount := 0
	if pollInfo != nil {
		if len(pollInfo) == 1 {
			pollTimeOut = pollInfo[0]
		} else {
			pollInterval = pollInfo[0]
			pollTimeOut = pollInfo[1]
		}
	}
	gomega.Expect(ip).NotTo(gomega.BeNil())
	dialTimeout := 2 * time.Second

	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, ip)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	waitErr := wait.PollUntilContextTimeout(context.Background(), pollInterval, pollTimeOut, true,
		func(ctx context.Context) (bool, error) {
			_, err := net.DialTimeout("tcp", addr, dialTimeout)
			if err != nil {
				framework.Logf("host %s unreachable, error: %s", addr, err.Error())
				return false, nil
			} else {
				framework.Logf("host %s is reachable", addr)
				hostReachableCount += 1
			}
			// checking if host is reachable 5 times
			if hostReachableCount == 5 {
				framework.Logf("host %s is reachable atleast 5 times", addr)
				return true, nil
			}
			return false, nil
		})
	return waitErr
}

// This function wait for host to be down
func WaitForHostToBeDown(ctx context.Context, vs *config.E2eTestConfig, ip string) error {
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

// This function powers on esxi hosts which were powered off cluster wise
func PowerOnEsxiHostByCluster(hostToPowerOn string, vs *config.E2eTestConfig) {
	var esxHostIp string = ""
	for _, esxInfo := range vs.TestInput.TestBedInfo.EsxHosts {
		if hostToPowerOn == esxInfo["vmName"] {
			esxHostIp = esxInfo["ip"]
			err := nimbus.VMPowerMgmt(vs.TestInput.TestBedInfo.User,
				vs.TestInput.TestBedInfo.Location, vs.TestInput.TestBedInfo.Podname,
				hostToPowerOn, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
	}
	err := WaitForHostToBeUp(esxHostIp, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// This function takes cluster name as input parameter and powers off esxi host of that cluster
func PowerOffEsxiHostByCluster(ctx context.Context, vs *config.E2eTestConfig, clusterName string,
	esxCount int) []string {
	var powerOffHostsList []string
	var hostsInCluster []*object.HostSystem
	clusterComputeResource, _, err := GetClusterName(ctx, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	hostsInCluster = GetHostsByClusterName(ctx, clusterComputeResource, clusterName)
	for i := 0; i < esxCount; i++ {
		for _, esxInfo := range vs.TestInput.TestBedInfo.EsxHosts {
			host := hostsInCluster[i].Common.InventoryPath
			hostIp := strings.Split(host, "/")
			if hostIp[len(hostIp)-1] == esxInfo["ip"] {
				esxHostName := esxInfo["vmName"]
				powerOffHostsList = append(powerOffHostsList, esxHostName)
				err = nimbus.VMPowerMgmt(vs.TestInput.TestBedInfo.User,
					vs.TestInput.TestBedInfo.Location, vs.TestInput.TestBedInfo.Podname,
					esxHostName, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = WaitForHostToBeDown(ctx, vs, esxInfo["ip"])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	return powerOffHostsList
}

// This function returns list of hosts and it takes clusterComputeResource as input.
func GetHostsByClusterName(ctx context.Context, clusterComputeResource []*object.ClusterComputeResource,
	clusterName string) []*object.HostSystem {
	var err error
	computeCluster := clusterName
	if computeCluster == "" {
		framework.Logf("Cluster name is either wrong or empty, returning nil hosts")
		return nil
	}
	var hosts []*object.HostSystem
	for _, cluster := range clusterComputeResource {
		if strings.Contains(computeCluster, cluster.Name()) {
			hosts, err = cluster.Hosts(ctx)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	gomega.Expect(hosts).NotTo(gomega.BeNil())
	return hosts
}

// This function returns default datastore.
func GetDefaultDatastore(ctx context.Context,
	vs *config.E2eTestConfig, forceRefresh ...bool) *object.Datastore {
	refresh := false
	if len(forceRefresh) > 0 {
		refresh = forceRefresh[0]
	}
	if vs.TestInput.TestBedInfo.DefaultDatastore == nil || refresh {
		finder := find.NewFinder(vs.VcClient.Client, false)
		cfg := vs.TestInput
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		datacenters := []string{}
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			framework.Logf("Looking for default datastore in DC: %q", dc)
			datastoreURL := env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
			vs.TestInput.TestBedInfo.DefaultDatastore, err = GetDatastoreByURL(ctx, vs, datastoreURL, defaultDatacenter)
			if err == nil {
				framework.Logf("Datstore found for DS URL:%q", datastoreURL)
				break
			}
		}
		gomega.Expect(vs.TestInput.TestBedInfo.DefaultDatastore).NotTo(gomega.BeNil())
	}

	return vs.TestInput.TestBedInfo.DefaultDatastore
}

// This function retry fcd deletion when a
// specific error is encountered.
func DeleteFcdWithRetriesForSpecificErr(ctx context.Context, vs *config.E2eTestConfig, fcdID string,
	dsRef vim25types.ManagedObjectReference, errsToIgnore []string, errsToContinue []string) error {
	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll*15, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			framework.Logf("Trying to delete FCD: %s", fcdID)
			err = DeleteFCD(ctx, fcdID, vs, dsRef)
			if err != nil {
				for _, errToIgnore := range errsToIgnore {
					if strings.Contains(err.Error(), errToIgnore) {
						// In FCD, there is a background thread that makes calls to host
						// to sync datastore every minute.
						framework.Logf("Hit error '%s' while trying to delete FCD: %s, will retry after %v seconds ...",
							err.Error(), fcdID, constants.Poll*15)
						return false, nil
					}
				}
				for _, errToContinue := range errsToContinue {
					if strings.Contains(err.Error(), errToContinue) {
						framework.Logf("Hit error '%s' while trying to delete FCD: %s, "+
							"will ignore this error(treat as success) and proceed to next steps...",
							err.Error(), fcdID)
						return true, nil
					}
				}
				return false, err
			}
			return true, nil
		})
	return waitErr
}

// This function call CnsQueryVolume and returns CnsQueryResult to client
func QueryCNSVolumeWithResult(vs *config.E2eTestConfig, fcdID string) (*cnstypes.CnsQueryResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Connect to VC
	connections.ConnectToVC(ctx, vs)
	var volumeIds []cnstypes.CnsVolumeId
	volumeIds = append(volumeIds, cnstypes.CnsVolumeId{
		Id: fcdID,
	})
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  100,
		},
	}
	req := cnstypes.CnsQueryVolume{
		This:   cnsclient.CnsVolumeManagerInstance,
		Filter: &queryFilter,
	}

	err := connections.ConnectCns(ctx, vs)
	if err != nil {
		return nil, err
	}
	res, err := cnsmethods.CnsQueryVolume(ctx, vs.CnsClient.Client, &req)
	if err != nil {
		return nil, err
	}
	return &res.Returnval, nil
}

// This function returns a map of VM ips for kubernetes nodes and its moid
func VmIpToMoRefMap(ctx context.Context, vs *config.E2eTestConfig) map[string]vim25types.ManagedObjectReference {
	if vs.TestInput.VMMor.VmIp2MoMap != nil {
		return vs.TestInput.VMMor.VmIp2MoMap
	}
	vs.TestInput.VMMor.VmIp2MoMap = make(map[string]vim25types.ManagedObjectReference)
	vmObjs := GetAllVms(ctx, vs)
	for _, mo := range vmObjs {
		if !strings.Contains(mo.Name(), "k8s") {
			continue
		}
		ip, err := mo.WaitForIP(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ip).NotTo(gomega.BeEmpty())
		vs.TestInput.VMMor.VmIp2MoMap[ip] = mo.Reference()
		framework.Logf("VM with IP %s is named %s and its moid is %s", ip, mo.Name(), mo.Reference().Value)
	}
	return vs.TestInput.VMMor.VmIp2MoMap

}

// This function use CnsQuerySnapshots
// and returns CnsSnapshotQueryResult to client
func QueryCNSVolumeSnapshotWithResult(vs *config.E2eTestConfig, fcdID string,
	snapshotId string) (*cnstypes.CnsSnapshotQueryResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var snapshotSpec []cnstypes.CnsSnapshotQuerySpec
	snapshotSpec = append(snapshotSpec, cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: fcdID,
		},
		SnapshotId: &cnstypes.CnsSnapshotId{
			Id: snapshotId,
		},
	})

	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: snapshotSpec,
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  100,
		},
	}

	req := cnstypes.CnsQuerySnapshots{
		This:                cnsclient.CnsVolumeManagerInstance,
		SnapshotQueryFilter: queryFilter,
	}

	res, err := cnsmethods.CnsQuerySnapshots(ctx, vs.CnsClient.Client, &req)
	if err != nil {
		return nil, err
	}

	task, err := object.NewTask(vs.VcClient.Client, res.Returnval), nil
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskInfo, err := cns.GetTaskInfo(ctx, task)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskResult, err := cns.GetQuerySnapshotsTaskResult(ctx, taskInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return taskResult, nil
}

// verifySnapshotIsDeletedInCNS verifies the snapshotId's presence on CNS
func VerifySnapshotIsDeletedInCNS(vs *config.E2eTestConfig, volumeId string, snapshotId string) error {
	ginkgo.By(fmt.Sprintf("Invoking queryCNSVolumeSnapshotWithResult with VolumeID: %s and SnapshotID: %s",
		volumeId, snapshotId))
	var querySnapshotResult *cnstypes.CnsSnapshotQueryResult
	var err error
	querySnapshotResult, err = QueryCNSVolumeSnapshotWithResult(vs, volumeId, snapshotId)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Task result is %+v", querySnapshotResult))
	gomega.Expect(querySnapshotResult.Entries).ShouldNot(gomega.BeEmpty())
	if querySnapshotResult.Entries[0].Snapshot.SnapshotId.Id != "" {
		return fmt.Errorf("snapshot entry is still present in CNS %s",
			querySnapshotResult.Entries[0].Snapshot.SnapshotId.Id)
	}
	return nil
}

// This function verifies the snapshotId's presence on CNS
func VerifySnapshotIsCreatedInCNS(vs *config.E2eTestConfig, volumeId string, snapshotId string) error {
	ginkgo.By(fmt.Sprintf("Invoking queryCNSVolumeSnapshotWithResult with VolumeID: %s and SnapshotID: %s",
		volumeId, snapshotId))
	var querySnapshotResult *cnstypes.CnsSnapshotQueryResult
	var err error
	querySnapshotResult, err = QueryCNSVolumeSnapshotWithResult(vs, volumeId, snapshotId)

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Task result is %+v", querySnapshotResult))
	gomega.Expect(querySnapshotResult.Entries).ShouldNot(gomega.BeEmpty())
	if querySnapshotResult.Entries[0].Snapshot.SnapshotId.Id != snapshotId {
		return fmt.Errorf("snapshot entry is not present in CNS %s", snapshotId)
	}
	return nil
}

// getAllDatacenters returns all the DataCenter Objects
func GetAllDatacenters(ctx context.Context, vs *config.E2eTestConfig) ([]*object.Datacenter, error) {
	connections.ConnectToVC(ctx, vs)
	finder := find.NewFinder(vs.VcClient.Client, false)
	return finder.DatacenterList(ctx, "*")
}

// This function returns the DataCenter Object for the given datacenterPath
func GetDatacenter(ctx context.Context, vs *config.E2eTestConfig, datacenterPath string) (*object.Datacenter, error) {
	connections.ConnectToVC(ctx, vs)
	finder := find.NewFinder(vs.VcClient.Client, false)
	return finder.Datacenter(ctx, datacenterPath)
}

// This function returns the datastore references of all the datastores mounted on the specified host
func GetDatastoresMountedOnHost(ctx context.Context, vs *config.E2eTestConfig,
	host vim25types.ManagedObjectReference) []vim25types.ManagedObjectReference {
	connections.ConnectToVC(ctx, vs)
	var hostMo mo.HostSystem
	err := vs.VcClient.RetrieveOne(ctx, host, []string{"datastore"}, &hostMo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return hostMo.Datastore
}

// This function gets the VM object Reference from the given vmUUID
func GetVMByUUID(ctx context.Context, vs *config.E2eTestConfig, vmUUID string) (object.Reference, error) {
	connections.ConnectToVC(ctx, vs)
	dcList, err := GetAllDatacenters(ctx, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, dc := range dcList {
		datacenter := object.NewDatacenter(vs.VcClient.Client, dc.Reference())
		s := object.NewSearchIndex(vs.VcClient.Client)
		vmUUID = strings.ToLower(strings.TrimSpace(vmUUID))
		instanceUUID := !(vs.TestInput.ClusterFlavor.VanillaCluster || vs.TestInput.ClusterFlavor.GuestCluster)
		vmMoRef, err := s.FindByUuid(ctx, datacenter, vmUUID, true, &instanceUUID)

		if err != nil || vmMoRef == nil {
			continue
		}
		return vmMoRef, nil
	}
	framework.Logf("err in getVMByUUID is %+v for vmuuid: %s", err, vmUUID)
	return nil, fmt.Errorf("node VM with UUID:%s is not found", vmUUID)
}

// This function returns host object reference of the host on which the specified VM resides
func GetHostFromVMReference(ctx context.Context, vs *config.E2eTestConfig,
	vm vim25types.ManagedObjectReference) vim25types.ManagedObjectReference {
	connections.ConnectToVC(ctx, vs)
	var vmMo mo.VirtualMachine
	err := vs.VcClient.RetrieveOne(ctx, vm, []string{"summary.runtime.host"}, &vmMo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	host := *vmMo.Summary.Runtime.Host
	return host
}

// This function gets the VM object Reference from the given vmUUID with a given wait timeout
func GetVMByUUIDWithWait(ctx context.Context, vs *config.E2eTestConfig,
	vmUUID string, timeout time.Duration) (object.Reference, error) {
	connections.ConnectToVC(ctx, vs)
	dcList, err := GetAllDatacenters(ctx, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var vmMoRefForvmUUID object.Reference
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(constants.Poll) {
		var vmMoRefFound bool
		for _, dc := range dcList {
			datacenter := object.NewDatacenter(vs.VcClient.Client, dc.Reference())
			s := object.NewSearchIndex(vs.VcClient.Client)
			vmUUID = strings.ToLower(strings.TrimSpace(vmUUID))
			instanceUUID := !(vs.TestInput.ClusterFlavor.VanillaCluster || vs.TestInput.ClusterFlavor.GuestCluster)
			vmMoRef, err := s.FindByUuid(ctx, datacenter, vmUUID, true, &instanceUUID)

			if err != nil || vmMoRef == nil {
				continue
			}
			if vmMoRef != nil {
				vmMoRefFound = true
				vmMoRefForvmUUID = vmMoRef
			}
		}
		if vmMoRefFound {
			framework.Logf("vmuuid: %s still exists", vmMoRefForvmUUID)
			continue
		} else {
			return nil, fmt.Errorf("node VM with UUID:%s is not found", vmUUID)
		}
	}
	return vmMoRefForvmUUID, nil
}

// This function checks volume is attached to the VM by vmUUID.
// This function returns true if volume is attached to the VM, else returns false
func IsVolumeAttachedToVM(client clientset.Interface,
	vs *config.E2eTestConfig, volumeID string, vmUUID string) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vmRef, err := GetVMByUUID(ctx, vs, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("vmRef: %v for the VM uuid: %s", vmRef, vmUUID)
	gomega.Expect(vmRef).NotTo(gomega.BeNil(), "vmRef should not be nil")
	vm := object.NewVirtualMachine(vs.VcClient.Client, vmRef.Reference())
	device, err := GetVirtualDeviceByDiskID(ctx, vs, vm, volumeID)
	if err != nil {
		framework.Logf("failed to determine whether disk %q is still attached to the VM with UUID: %q", volumeID, vmUUID)
		return false, err
	}
	if device == nil {
		return false, nil
	}
	framework.Logf("Found the disk %q is attached to the VM with UUID: %q", volumeID, vmUUID)
	return true, nil
}

// This function gets the virtual device by diskID.
func GetVirtualDeviceByDiskID(ctx context.Context, vs *config.E2eTestConfig, vm *object.VirtualMachine,
	diskID string) (vim25types.BaseVirtualDevice, error) {
	vmname, err := vm.Common.ObjectName(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		framework.Logf("failed to get the devices for VM: %q. err: %+v", vmname, err)
		return nil, err
	}
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			if virtualDisk, ok := device.(*vim25types.VirtualDisk); ok {
				if virtualDisk.VDiskId != nil && virtualDisk.VDiskId.Id == diskID {
					framework.Logf("Found FCDID %q attached to VM %q", diskID, vmname)
					return device, nil
				}
			}
		}
	}
	framework.Logf("failed to find FCDID %q attached to VM %q", diskID, vmname)
	return nil, nil
}

// This function returns the namespace in which the tests are expected
// to run. For Vanilla & GuestCluster test setups, returns random namespace name
// generated by the framework. For SupervisorCluster test setup, returns the
// user created namespace where pod vms will be provisioned.
func GetNamespaceToRunTests(f *framework.Framework, vs *config.E2eTestConfig) string {
	if vs.TestInput.ClusterFlavor.SupervisorCluster {
		return env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace)
	}
	return f.Namespace.Name
}

// This function verifies if  volume is created with specified storagePolicyName
func VerifySpbmPolicyOfVolume(volumeID string, vs *config.E2eTestConfig, storagePolicyName string) (bool, error) {
	framework.Logf("Verifying volume: %s is created using storage policy: %s", volumeID, storagePolicyName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to VC
	connections.ConnectToVC(ctx, vs)

	// Get PBM Client
	pbmClient, err := pbm.NewClient(ctx, vs.VcClient.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	profileID, err := pbmClient.ProfileIDByName(ctx, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("storage policy id: %s for storage policy name is: %s", profileID, storagePolicyName)
	ProfileID :=
		pbmtypes.PbmProfileId{
			UniqueId: profileID,
		}

	associatedDisks, err := pbmClient.QueryAssociatedEntity(ctx, ProfileID, constants.VirtualDiskUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(associatedDisks).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to find associated disks for storage policy: %s", profileID))
	for _, ad := range associatedDisks {
		if ad.Key == volumeID {
			framework.Logf("Volume: %s is associated with storage policy: %s", volumeID, profileID)
			return true, nil
		}
	}
	framework.Logf("Volume: %s is NOT associated with storage policy: %s", volumeID, profileID)
	return false, nil
}

// This function returns profile ID for the specified storagePolicyName
func GetSpbmPolicyID(storagePolicyName string, vs *config.E2eTestConfig) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get PBM Client
	pbmClient, err := pbm.NewClient(ctx, vs.VcClient.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	profileID, err := pbmClient.ProfileIDByName(ctx, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get profileID from given profileName")
	framework.Logf("storage policy id: %s for storage policy name is: %s", profileID, storagePolicyName)
	return profileID
}

// This function executes QueryVolume API on vCenter for requested volumeid and returns
// volume labels for requested entityType, entityName and entityNamespace
func GetLabelsForCNSVolume(vs *config.E2eTestConfig, volumeID string, entityType string,
	entityName string, entityNamespace string) (map[string]string, error) {
	queryResult, err := QueryCNSVolumeWithResult(vs, volumeID)
	if err != nil {
		return nil, err
	}
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return nil, fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		if kubernetesMetadata.EntityType == entityType && kubernetesMetadata.EntityName == entityName &&
			kubernetesMetadata.Namespace == entityNamespace {
			return GetLabelsMapFromKeyValue(kubernetesMetadata.Labels), nil
		}
	}
	return nil, fmt.Errorf("entity %s with name %s not found in namespace %s for volume %s",
		entityType, entityName, entityNamespace, volumeID)
}

// This function executes QueryVolume API on vCenter and verifies
// volume labels are updated by metadata-syncer
func WaitForLabelsToBeUpdated(vs *config.E2eTestConfig, volumeID string, matchLabels map[string]string,
	entityType string, entityName string, entityNamespace string) error {
	err := wait.PollUntilContextTimeout(context.Background(), constants.Poll, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			err := VerifyLabelsAreUpdated(vs, volumeID, matchLabels, entityType, entityName, entityNamespace)
			if err == nil {
				return true, nil
			} else {
				return false, nil
			}
		})
	if err != nil {
		if wait.Interrupted(err) {
			return fmt.Errorf("labels are not updated to %+v for %s %q for volume %s",
				matchLabels, entityType, entityName, volumeID)
		}
		return err
	}

	return nil
}

// This function executes QueryVolume API on vCenter and verifies
// volume metadata for given volume has been deleted
func WaitForMetadataToBeDeleted(vs *config.E2eTestConfig, volumeID string, entityType string,
	entityName string, entityNamespace string) error {
	err := wait.PollUntilContextTimeout(context.Background(), constants.Poll, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			queryResult, err := QueryCNSVolumeWithResult(vs, volumeID)
			framework.Logf("queryResult: %s", spew.Sdump(queryResult))
			if err != nil {
				return true, err
			}
			if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
				return true, fmt.Errorf("failed to query cns volume %s", volumeID)
			}
			gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
			for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
				if metadata == nil {
					continue
				}
				kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
				if kubernetesMetadata.EntityType == entityType && kubernetesMetadata.EntityName == entityName &&
					kubernetesMetadata.Namespace == entityNamespace {
					return false, nil
				}
			}
			return true, nil
		})
	if err != nil {
		if wait.Interrupted(err) {
			return fmt.Errorf("entityName %s of entityType %s is not deleted for volume %s",
				entityName, entityType, volumeID)
		}
		return err
	}

	return nil
}

// This function executes QueryVolume API on vCenter and verifies
// volume entries are deleted from vCenter Database
func WaitForCNSVolumeToBeDeleted(vs *config.E2eTestConfig, volumeID string) error {
	err := wait.PollUntilContextTimeout(context.Background(), constants.Poll, 2*constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			queryResult, err := QueryCNSVolumeWithResult(vs, volumeID)
			if err != nil {
				return true, err
			}

			if len(queryResult.Volumes) == 0 {
				framework.Logf("volume %q has successfully deleted", volumeID)
				return true, nil
			}
			framework.Logf("waiting for Volume %q to be deleted.", volumeID)
			return false, nil
		})
	if err != nil {
		return err
	}
	return nil
}

// This function executes QueryVolume API on vCenter and verifies
// volume entries are created in vCenter Database
func WaitForCNSVolumeToBeCreated(vs *config.E2eTestConfig, volumeID string) error {
	err := wait.PollUntilContextTimeout(context.Background(), constants.Poll, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			queryResult, err := QueryCNSVolumeWithResult(vs, volumeID)
			if err != nil {
				return true, err
			}

			if len(queryResult.Volumes) == 1 && queryResult.Volumes[0].VolumeId.Id == volumeID {
				framework.Logf("volume %q has successfully created", volumeID)
				return true, nil
			}
			framework.Logf("waiting for Volume %q to be created.", volumeID)
			return false, nil
		})
	return err
}

// This function creates an FCD disk
func CreateFCD(ctx context.Context, vs *config.E2eTestConfig, fcdname string,
	diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference) (string, error) {
	KeepAfterDeleteVM := false
	spec := vim25types.VslmCreateSpec{
		Name:              fcdname,
		CapacityInMB:      diskCapacityInMB,
		KeepAfterDeleteVm: &KeepAfterDeleteVM,
		BackingSpec: &vim25types.VslmCreateSpecDiskFileBackingSpec{
			VslmCreateSpecBackingSpec: vim25types.VslmCreateSpecBackingSpec{
				Datastore: dsRef,
			},
			ProvisioningType: string(vim25types.BaseConfigInfoDiskFileBackingInfoProvisioningTypeThin),
		},
	}
	req := vim25types.CreateDisk_Task{
		This: *vs.VcClient.Client.ServiceContent.VStorageObjectManager,
		Spec: spec,
	}
	res, err := methods.CreateDisk_Task(ctx, vs.VcClient.Client, &req)
	if err != nil {
		return "", err
	}
	task := object.NewTask(vs.VcClient.Client, res.Returnval)
	taskInfo, err := task.WaitForResultEx(ctx, nil)
	if err != nil {
		return "", err
	}
	fcdID := taskInfo.Result.(vim25types.VStorageObject).Config.Id.Id
	return fcdID, nil
}

// This function with valid storage policy
func CreateFCDwithValidProfileID(ctx context.Context, vs *config.E2eTestConfig, fcdname string,
	profileID string, diskCapacityInMB int64, dsRef vim25types.ManagedObjectReference) (string, error) {
	KeepAfterDeleteVM := false
	spec := vim25types.VslmCreateSpec{
		Name:              fcdname,
		CapacityInMB:      diskCapacityInMB,
		KeepAfterDeleteVm: &KeepAfterDeleteVM,
		BackingSpec: &vim25types.VslmCreateSpecDiskFileBackingSpec{
			VslmCreateSpecBackingSpec: vim25types.VslmCreateSpecBackingSpec{
				Datastore: dsRef,
			},
			ProvisioningType: string(vim25types.BaseConfigInfoDiskFileBackingInfoProvisioningTypeThin),
		},
		Profile: []vim25types.BaseVirtualMachineProfileSpec{
			&vim25types.VirtualMachineDefinedProfileSpec{
				ProfileId: profileID,
			},
		},
	}
	req := vim25types.CreateDisk_Task{
		This: *vs.VcClient.Client.ServiceContent.VStorageObjectManager,
		Spec: spec,
	}
	res, err := methods.CreateDisk_Task(ctx, vs.VcClient.Client, &req)
	if err != nil {
		return "", err
	}
	task := object.NewTask(vs.VcClient.Client, res.Returnval)
	taskInfo, err := task.WaitForResultEx(ctx, nil)
	if err != nil {
		return "", err
	}
	fcdID := taskInfo.Result.(vim25types.VStorageObject).Config.Id.Id
	return fcdID, nil
}

// This function deletes an FCD disk
func DeleteFCD(ctx context.Context, fcdID string,
	vs *config.E2eTestConfig, dsRef vim25types.ManagedObjectReference) error {
	req := vim25types.DeleteVStorageObject_Task{
		This:      *vs.VcClient.Client.ServiceContent.VStorageObjectManager,
		Datastore: dsRef,
		Id:        vim25types.ID{Id: fcdID},
	}
	res, err := methods.DeleteVStorageObject_Task(ctx, vs.VcClient.Client, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(vs.VcClient.Client, res.Returnval)
	_, err = task.WaitForResultEx(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// This function relocates an FCD disk
func RelocateFCD(ctx context.Context, vs *config.E2eTestConfig, fcdID string,
	dsRefSrc vim25types.ManagedObjectReference, dsRefDest vim25types.ManagedObjectReference) error {
	spec := vim25types.VslmRelocateSpec{
		VslmMigrateSpec: vim25types.VslmMigrateSpec{
			DynamicData: vim25types.DynamicData{},
			BackingSpec: &vim25types.VslmCreateSpecDiskFileBackingSpec{
				VslmCreateSpecBackingSpec: vim25types.VslmCreateSpecBackingSpec{
					Datastore: dsRefDest,
				},
			},
		},
	}
	req := vim25types.RelocateVStorageObject_Task{
		This:      *vs.VcClient.Client.ServiceContent.VStorageObjectManager,
		Id:        vim25types.ID{Id: fcdID},
		Datastore: dsRefSrc,
		Spec:      spec,
	}
	res, err := methods.RelocateVStorageObject_Task(ctx, vs.VcClient.Client, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(vs.VcClient.Client, res.Returnval)
	_, err = task.WaitForResultEx(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// This function verify file volume properties like
// capacity, volume type, datastore type and datacenter.
func VerifyVolPropertiesFromCnsQueryResults(vs *config.E2eTestConfig, volHandle string, testVolumeType string) {

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := QueryCNSVolumeWithResult(vs, volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	ginkgo.By(fmt.Sprintf("volume Name:%s , capacity:%d volumeType:%s health:%s accesspoint: %s",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints))

	// Verifying disk size specified in PVC is honored
	ginkgo.By("Verifying disk size specified in PVC is honored")
	gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).
		CapacityInMb == constants.DiskSizeInMb).To(gomega.BeTrue(), "wrong disk size provisioned")

	// Verifying volume type specified in PVC is honored
	ginkgo.By("Verifying volume type specified in PVC is honored")
	gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(), "volume type is not FILE")

	// Verify if VolumeID is created on the VSAN datastores
	ginkgo.By("Verify if VolumeID is created on the VSAN datastores")
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).
		To(gomega.BeTrue(), "Volume is not provisioned on vSan datastore")

	// Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf
	ginkgo.By("Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf")
	gomega.Expect(IsDatastoreBelongsToDatacenterSpecifiedInConfig(vs, queryResult.Volumes[0].DatastoreUrl)).
		To(gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file")

}

// This function checks whether the given datastoreURL belongs
// to the datacenter specified in the vSphere.conf file.
func IsDatastoreBelongsToDatacenterSpecifiedInConfig(vs *config.E2eTestConfig, datastoreURL string) bool {
	var datacenters []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finder := find.NewFinder(vs.VcClient.Client, false)
	cfg := vs.TestInput
	dcList := strings.Split(cfg.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}
	for _, dc := range datacenters {
		defaultDatacenter, _ := finder.Datacenter(ctx, dc)
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err := GetDatastoreByURL(ctx, vs, datastoreURL, defaultDatacenter)
		if defaultDatastore != nil && err == nil {
			return true
		}
	}

	// Loop through all datacenters specified in conf file, and cannot find this
	// given datastore.
	return false
}

// This function returns the *Datastore instance given its URL.
func GetDatastoreByURL(ctx context.Context,
	vs *config.E2eTestConfig, datastoreURL string,
	dc *object.Datacenter) (*object.Datastore, error) {
	finder := find.NewFinder(dc.Client(), false)
	finder.SetDatacenter(dc)
	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		framework.Logf("failed to get all the datastores. err: %+v", err)
		return nil, err
	}
	var dsList []vim25types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}

	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(dc.Client())
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		framework.Logf("failed to get Datastore managed objects from datastore objects."+
			" dsObjList: %+v, properties: %+v, err: %v", dsList, properties, err)
		return nil, err
	}
	for _, dsMo := range dsMoList {
		if dsMo.Info.GetDatastoreInfo().Url == datastoreURL {
			return object.NewDatastore(dc.Client(),
				dsMo.Reference()), nil
		}
	}
	err = fmt.Errorf("couldn't find Datastore given URL %q", datastoreURL)
	return nil, err
}

// This function returns the clusterComputeResource and vSANClient.
func GetClusterComputeResource(ctx context.Context,
	vs *config.E2eTestConfig) ([]*object.ClusterComputeResource, *vsan.VsanClient) {
	var err error
	clusterComputeResource, vsanHealthClient, err := GetClusterName(ctx, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return clusterComputeResource, vsanHealthClient
}

// This function returns the cluster and vsan client of the testbed
func GetClusterName(ctx context.Context,
	vs *config.E2eTestConfig) ([]*object.ClusterComputeResource, *vsan.VsanClient, error) {
	c := vc.NewClient(ctx, vs.TestInput.Global.VCenterHostname,
		vs.TestInput.Global.VCenterPort, vs.TestInput.Global.User, vs.TestInput.Global.Password)
	datacenter := vs.TestInput.Global.Datacenters
	vsanHealthClient, err := vsan.NewVsanHealthSvcClient(ctx, c.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	finder := find.NewFinder(vsanHealthClient.Vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)

	clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
	framework.Logf("clusterComputeResource %v", clusterComputeResource)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return clusterComputeResource, vsanHealthClient, err
}

// This function takes input of the HostInfo which has host uuid
// with the host uuid it maps the corresponding host IP and returns it
func GetHostUUID(ctx context.Context, vs *config.E2eTestConfig, hostInfo string) string {
	var result map[string]interface{}
	computeCluster := os.Getenv("CLUSTER_NAME")
	if computeCluster == "" {
		if vs.TestInput.ClusterFlavor.GuestCluster {
			computeCluster = "compute-cluster"
		} else if vs.TestInput.ClusterFlavor.SupervisorCluster {
			computeCluster = "wcp-app-platform-sanity-cluster"
		}
		framework.Logf("Default cluster is chosen for test")
	}

	err := json.Unmarshal([]byte(hostInfo), &result)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	lsomObject := result["lsom_objects"]
	lsomObjectInterface, _ := lsomObject.(map[string]interface{})

	// This loop get the the esx host uuid from the queryVsanObj result
	for _, lsomValue := range lsomObjectInterface {
		key, _ := lsomValue.(map[string]interface{})
		for key1, value1 := range key {
			if key1 == "owner" {
				framework.Logf("hostUUID %v, hostIP %v", key1, value1)
				finder := find.NewFinder(vs.VcClient.Client, false)
				dc, err := finder.Datacenter(ctx, vs.TestInput.Global.Datacenters)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				finder.SetDatacenter(dc)

				clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				cluster := clusterComputeResource[0]
				// TKG setup with NSX has edge-cluster enabled, this check is to skip that cluster
				if !strings.Contains(cluster.Name(), computeCluster) {
					cluster = clusterComputeResource[1]
				}

				config, err := cluster.Configuration(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for counter := range config.VsanHostConfig {
					if config.VsanHostConfig[counter].ClusterInfo.NodeUuid == value1 {
						hosts, err := cluster.Hosts(ctx)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						framework.Logf("host name from hostUUID %v", hosts[counter].Name())
						return hosts[counter].Name()
					}
				}
			}
		}
	}
	return ""
}

// This function takes vsanObjuuid as input and resturns vsanObj info such as
// hostUUID.
func QueryVsanObj(ctx context.Context, vs *config.E2eTestConfig, vsanObjuuid string) string {
	c := vc.NewClient(ctx, vs.TestInput.Global.VCenterHostname,
		vs.TestInput.Global.VCenterPort, vs.TestInput.Global.User, vs.TestInput.Global.Password)
	datacenter := vs.TestInput.Global.Datacenters

	vsanHealthClient, err := vsan.NewVsanHealthSvcClient(ctx, c.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	finder := find.NewFinder(vsanHealthClient.Vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	result, err := QueryVsanObjects(ctx, []string{vsanObjuuid}, vs, vsanHealthClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return result
}

// This function invokes reboot command on the given vCenter over SSH.
func InvokeVCenterReboot(ctx context.Context, vs *config.E2eTestConfig, host string) error {
	sshCmd := "reboot"
	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
	result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return err
}

// This function uses govc command to fetch the host name where
// vm is present
func GetHostIpWhereVmIsPresent(vs *config.E2eTestConfig, vmIp string) string {
	vcAddress := vs.TestInput.TestBedInfo.VcAddress
	dc := env.GetAndExpectStringEnvVar(constants.Datacenter)
	vcAdminPwd := env.GetAndExpectStringEnvVar(constants.VcUIPwd)
	govcCmd := "export GOVC_INSECURE=1;"
	govcCmd += fmt.Sprintf("export GOVC_URL='https://administrator@vsphere.local:%s@%s';",
		vcAdminPwd, vcAddress)
	govcCmd += fmt.Sprintf("govc vm.info --vm.ip=%s -dc=%s;", vmIp, dc)
	framework.Logf("Running command: %s", govcCmd)
	result, err := exec.Command("/bin/bash", "-c", govcCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("res is: %v", result)
	hostIp := strings.Split(string(result), "Host:")
	host := strings.TrimSpace(hostIp[1])
	return host
}

// This function returns the vsanObjectsUUID.
func VsanObjIndentities(ctx context.Context, vs *config.E2eTestConfig, pvName string) string {
	var vsanObjUUID string
	computeCluster := os.Getenv(constants.EnvComputeClusterName)
	if computeCluster == "" {
		if vs.TestInput.ClusterFlavor.GuestCluster {
			computeCluster = "compute-cluster"
		} else if vs.TestInput.ClusterFlavor.SupervisorCluster {
			computeCluster = "wcp-app-platform-sanity-cluster"
		}
		framework.Logf("Default cluster is chosen for test")
	}
	clusterComputeResource, vsanHealthClient := GetClusterComputeResource(ctx, vs)
	for _, cluster := range clusterComputeResource {
		if strings.Contains(cluster.Name(), computeCluster) {
			// Fix for NotAuthenticated issue
			//bootstrap()

			clusterConfig, err := VsanQueryObjectIdentities(ctx, vsanHealthClient, cluster.Reference())
			framework.Logf("clusterconfig %v", clusterConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for index := range clusterConfig.Identities {
				if strings.Contains(clusterConfig.Identities[index].Description, pvName) {
					vsanObjUUID = clusterConfig.Identities[index].Uuid
					framework.Logf("vsanObjUUID is %v", vsanObjUUID)
					break
				}
			}
		}
	}
	gomega.Expect(vsanObjUUID).NotTo(gomega.BeNil())
	return vsanObjUUID
}

// This function return list of vsan uuids
// example: For a PVC, It returns the vSAN object UUIDs to their identities
// It return vsanObjuuid like [4336525f-7813-d78a-e3a4-02005456da7e]
func VsanQueryObjectIdentities(ctx context.Context, c *vsan.VsanClient,
	cluster vim25types.ManagedObjectReference) (*vsantypes.VsanObjectIdentityAndHealth, error) {

	// Creates the vsan object identities instance. This is to be queried from vsan health.
	var (
		VsanQueryObjectIdentitiesInstance = vim25types.ManagedObjectReference{
			Type:  "VsanObjectSystem",
			Value: "vsan-cluster-object-system",
		}
	)
	req := vsantypes.VsanQueryObjectIdentities{
		This:    VsanQueryObjectIdentitiesInstance,
		Cluster: &cluster,
	}

	res, err := vsanmethods.VsanQueryObjectIdentities(ctx, c.ServiceClient, &req)

	if err != nil {
		return nil, err
	}
	return res.Returnval, nil
}

// QueryVsanObjects takes vsan uuid as input and returns the vSANObj related
// information like lsom_objects and disk_objects.
// Example return values:
//
//	"{"disk_objects": {"525a9aa5-1142-4004-ad6f-2389eef25f06":
//	   ....lsom_objects": {"e7945f5f-4267-3e5d-334a-020063a7a5c4":......}
func QueryVsanObjects(ctx context.Context, uuids []string,
	vs *config.E2eTestConfig, vsanClient *vsan.VsanClient) (string, error) {
	computeCluster := os.Getenv("CLUSTER_NAME")
	if computeCluster == "" {
		if vs.TestInput.ClusterFlavor.GuestCluster {
			computeCluster = "compute-cluster"
		} else if vs.TestInput.ClusterFlavor.SupervisorCluster {
			computeCluster = "wcp-app-platform-sanity-cluster"
		}
		framework.Logf("Default cluster is chosen for test")
	}
	clusterComputeResource, _, err := GetClusterName(ctx, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cluster := clusterComputeResource[0]
	if !strings.Contains(cluster.Name(), computeCluster) {
		cluster = clusterComputeResource[1]
	}
	config, err := cluster.Configuration(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	hostSystem := config.VsanHostConfig[0].HostSystem.String()

	hostName := strings.Split(hostSystem, ":")
	hostValue := strings.Split(hostName[1], "-")
	value := ("ha-vsan-internal-system-" + hostValue[1])
	framework.Logf("vsan internal system value %v", value)
	var (
		QueryVsanObjectsInstance = vim25types.ManagedObjectReference{
			Type:  "HostVsanInternalSystem",
			Value: value,
		}
	)
	req := vim25types.QueryVsanObjects{
		This:  QueryVsanObjectsInstance,
		Uuids: uuids,
	}
	res, err := methods.QueryVsanObjects(ctx, vsanClient.ServiceClient, &req)
	if err != nil {
		framework.Logf("QueryVsanObjects Failed with err %v", err)
		return "", err
	}
	return res.Returnval, nil
}

// getRestConfigClient returns  rest config client.
func GetRestConfigClient(vs *config.E2eTestConfig) *rest.Config {
	// Get restConfig.
	var err error
	if vs.RestConfig == nil {
		if vs.TestInput.ClusterFlavor.SupervisorCluster || vs.TestInput.ClusterFlavor.VanillaCluster {
			k8senv := env.GetAndExpectStringEnvVar("KUBECONFIG")
			vs.RestConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
		}
		if vs.TestInput.ClusterFlavor.GuestCluster {
			if k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
				vs.RestConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
			}
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}
	return vs.RestConfig
}

/*
isVersionGreaterOrEqual returns true if presentVersion is equal to or greater than expectedVCversion
*/
func IsVersionGreaterOrEqual(presentVersion, expectedVCversion string) bool {
	// Split the version strings by dot
	v1Parts := strings.Split(presentVersion, ".")
	v2Parts := strings.Split(expectedVCversion, ".")

	// Compare parts
	for i := 0; i < len(v1Parts); i++ {
		v1, _ := strconv.Atoi(v1Parts[i]) // Convert each part to integer
		v2, _ := strconv.Atoi(v2Parts[i])

		if v1 > v2 {
			return true
		} else if v1 < v2 {
			return false
		}
	}
	return true // If all parts are equal, the versions are equal
}

/*
getVCversion returns the VC version
*/
func GetVCversion(ctx context.Context, vs *config.E2eTestConfig, vcAddress string) string {
	if vs.TestInput.TestBedInfo.VcVersion == "" {
		// Read hosts sshd port number
		ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo,
			vs.TestInput.TestBedInfo.VcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		addr := ip + ":" + portNum

		sshCmd := "vpxd -v"
		framework.Logf("Checking if fss is enabled on vCenter host %v", addr)
		result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
		fssh.LogResult(result)
		if err == nil && result.Code == 0 {
			vs.TestInput.TestBedInfo.VcVersion = strings.TrimSpace(result.Stdout)
		} else {
			ginkgo.By(fmt.Sprintf("couldn't execute command: %s on vCenter host: %v", sshCmd, err))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// Regex to find version in the format X.Y.Z
		re := regexp.MustCompile(`\d+\.\d+\.\d+`)
		vs.TestInput.TestBedInfo.VcVersion = re.FindString(vs.TestInput.TestBedInfo.VcVersion)
	}
	framework.Logf("vcVersion %s", vs.TestInput.TestBedInfo.VcVersion)
	return vs.TestInput.TestBedInfo.VcVersion
}

// getVsanClusterResource returns the vsan cluster's details
func GetVsanClusterResource(ctx context.Context,
	vs *config.E2eTestConfig, forceRefresh ...bool) *object.ClusterComputeResource {
	refresh := false
	var cluster *object.ClusterComputeResource
	if len(forceRefresh) > 0 {
		refresh = forceRefresh[0]
	}
	if vs.TestInput.TestBedInfo.DefaultCluster == nil || refresh {
		finder := find.NewFinder(vs.VcClient.Client, false)
		cfg, err := config.GetConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		datacenters := []string{}
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}

		for _, dc := range datacenters {
			defaultDatacenter, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)

			clusterComputeResource, err := finder.ClusterComputeResourceList(ctx, "*")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			cluster = clusterComputeResource[0]

			if strings.Contains(strings.ToLower(cluster.Name()), "edge") {
				cluster = clusterComputeResource[1]
			}

			framework.Logf("Looking for cluster with the default datastore passed into test in DC: %s", dc)
			datastoreURL := env.GetAndExpectStringEnvVar(constants.EnvSharedDatastoreURL)
			vs.TestInput.TestBedInfo.DefaultDatastore, err = GetDatastoreByURL(ctx, vs, datastoreURL, defaultDatacenter)
			if err == nil {
				framework.Logf("Cluster %s found matching the datastore URL: %s", clusterComputeResource[0].Name(),
					datastoreURL)
				vs.TestInput.TestBedInfo.DefaultCluster = cluster
				break
			}
		}
	} else {
		framework.Logf("Found the default Cluster already set")
	}
	framework.Logf("Returning cluster %s ", vs.TestInput.TestBedInfo.DefaultCluster.Name())
	return vs.TestInput.TestBedInfo.DefaultCluster
}

// getAllHostsIP reads cluster, gets hosts in it and returns IP array
func GetAllHostsIP(ctx context.Context, vs *config.E2eTestConfig, forceRefresh ...bool) []string {
	var result []string
	refresh := false
	if len(forceRefresh) > 0 {
		refresh = forceRefresh[0]
	}
	/*
	   I am not sure why this is used
	   bootstrap(false, true)
	*/

	cluster := GetVsanClusterResource(ctx, vs, refresh)
	hosts, err := cluster.Hosts(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, moHost := range hosts {
		result = append(result, moHost.Name())
	}
	return result
}

// getHostConnectionState reads cluster, gets hosts in it and returns Connection state of host
func GetHostConnectionState(ctx context.Context, vs *config.E2eTestConfig, addr string) (string, error) {
	var state string
	cluster := GetVsanClusterResource(ctx, vs, true)
	hosts, err := cluster.Hosts(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for counter := range hosts {
		var moHost mo.HostSystem
		if hosts[counter].Name() == addr {
			err = hosts[counter].Properties(ctx, hosts[counter].Reference(), []string{"summary"}, &moHost)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to get host system properties")
			framework.Logf("Host owning IP: %v, and its Connection state is %s", hosts[counter].Name(),
				moHost.Summary.Runtime.ConnectionState)
			state = string(moHost.Summary.Runtime.ConnectionState)
			return state, nil
		}
	}
	return "host not found", fmt.Errorf("host not found %s", addr)
}

// queryCNSVolumeWithWait gets the cns volume health status
func QueryCNSVolumeWithWait(ctx context.Context, vs *config.E2eTestConfig, volHandle string) error {
	waitErr := wait.PollUntilContextTimeout(ctx, constants.PollTimeoutShort, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			framework.Logf("wait for next poll %v", constants.PollTimeoutShort)

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
			queryResult, err := QueryCNSVolumeWithResult(vs, volHandle)
			gomega.Expect(len(queryResult.Volumes)).NotTo(gomega.BeZero())
			if err != nil {
				return false, nil
			}
			ginkgo.By("Verifying the volume health status returned by CNS(green/yellow/red")
			for _, vol := range queryResult.Volumes {
				if vol.HealthStatus == constants.HealthRed {
					framework.Logf("Volume health status: %v", vol.HealthStatus)
					return true, nil
				}
			}
			return false, nil
		})
	return waitErr
}

// waitForHostConnectionState gets the Connection state of the host and waits till the desired state is obtained
func WaitForHostConnectionState(ctx context.Context, vs *config.E2eTestConfig, addr string, state string) error {
	var output string
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {

			output, err := GetHostConnectionState(ctx, vs, addr)
			if err != nil {
				framework.Logf("The host's %s last seen state before returning error is : %s", addr, output)
				return false, nil
			}

			if state == output {
				framework.Logf("The host's %s current state is as expected state : %s", addr, output)
				return true, nil
			}
			return false, nil
		})
	framework.Logf("The host's %s last seen state before returning is : %s", addr, output)
	return waitErr
}

// deleteVolumeSnapshotInCNS Call deleteSnapshots API
func DeleteVolumeSnapshotInCNS(fcdID string, vs *config.E2eTestConfig, snapshotId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to VC
	connections.ConnectToVC(ctx, vs)

	var cnsSnapshotDeleteSpecList []cnstypes.CnsSnapshotDeleteSpec
	cnsSnapshotDeleteSpec := cnstypes.CnsSnapshotDeleteSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: fcdID,
		},
		SnapshotId: cnstypes.CnsSnapshotId{
			Id: snapshotId,
		},
	}
	cnsSnapshotDeleteSpecList = append(cnsSnapshotDeleteSpecList, cnsSnapshotDeleteSpec)

	req := cnstypes.CnsDeleteSnapshots{
		This:                cnsclient.CnsVolumeManagerInstance,
		SnapshotDeleteSpecs: cnsSnapshotDeleteSpecList,
	}

	res, err := cnsmethods.CnsDeleteSnapshots(ctx, vs.CnsClient.Client, &req)

	if err != nil {
		return err
	}

	task, err := object.NewTask(vs.VcClient.Client, res.Returnval), nil
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskInfo, err := cns.GetTaskInfo(ctx, task)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	deleteSnapshotsTaskResult, err := cns.GetTaskResult(ctx, taskInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	deleteSnapshotsOperationRes := deleteSnapshotsTaskResult.GetCnsVolumeOperationResult()

	if deleteSnapshotsOperationRes.Fault != nil {
		err = fmt.Errorf("failed to create snapshots: fault=%+v", deleteSnapshotsOperationRes.Fault)
	}

	snapshotDeleteResult := interface{}(deleteSnapshotsTaskResult).(*cnstypes.CnsSnapshotDeleteResult)
	framework.Logf("DeleteSnapshots: Snapshot deleted successfully. volumeId: %q, snapshot id %q",
		fcdID, snapshotDeleteResult.SnapshotId)
	return err
}

// createVolumeSnapshotInCNS Call createSnapshots API and returns snapshotId to client
func CreateVolumeSnapshotInCNS(vs *config.E2eTestConfig, fcdID string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cnsSnapshotCreateSpecList []cnstypes.CnsSnapshotCreateSpec
	cnsSnapshotCreateSpec := cnstypes.CnsSnapshotCreateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: fcdID,
		},
		Description: "Volume Snapshot created by CSI",
	}
	cnsSnapshotCreateSpecList = append(cnsSnapshotCreateSpecList, cnsSnapshotCreateSpec)

	req := cnstypes.CnsCreateSnapshots{
		This:          cnsclient.CnsVolumeManagerInstance,
		SnapshotSpecs: cnsSnapshotCreateSpecList,
	}

	res, err := cnsmethods.CnsCreateSnapshots(ctx, vs.CnsClient.Client, &req)

	if err != nil {
		return "", err
	}

	task, err := object.NewTask(vs.VcClient.Client, res.Returnval), nil
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskInfo, err := cns.GetTaskInfo(ctx, task)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	createSnapshotsOperationRes := taskResult.GetCnsVolumeOperationResult()

	if createSnapshotsOperationRes.Fault != nil {
		err = fmt.Errorf("failed to create snapshots: fault=%+v", createSnapshotsOperationRes.Fault)
	}

	snapshotCreateResult := interface{}(taskResult).(*cnstypes.CnsSnapshotCreateResult)
	snapshotId := snapshotCreateResult.Snapshot.SnapshotId.Id
	snapshotCreateTime := snapshotCreateResult.Snapshot.CreateTime
	framework.Logf("CreateSnapshot: Snapshot created successfully. volumeId: %q, snapshot id %q, time stamp %+v",
		fcdID, snapshotId, snapshotCreateTime)

	return snapshotId, err
}

// verifyVolumeCompliance verifies the volume policy compliance status
func VerifyVolumeCompliance(vs *config.E2eTestConfig, volumeID string, shouldBeCompliant bool) {
	queryResult, err := QueryCNSVolumeWithResult(vs, volumeID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Volume id: %v compliance status: %v", volumeID, queryResult.Volumes[0].ComplianceStatus)
	if shouldBeCompliant {
		gomega.Expect(queryResult.Volumes[0].ComplianceStatus == "compliant").To(gomega.BeTrue())
	} else {
		gomega.Expect(queryResult.Volumes[0].ComplianceStatus == "compliant").To(gomega.BeFalse())
	}
}

// verifyLabelsAreUpdated executes cns QueryVolume API on vCenter and verifies if
// volume labels are updated by metadata-syncer
func VerifyLabelsAreUpdated(vs *config.E2eTestConfig, volumeID string, matchLabels map[string]string,
	entityType string, entityName string, entityNamespace string) error {

	queryResult, err := QueryCNSVolumeWithResult(vs, volumeID)
	framework.Logf("queryResult: %s", spew.Sdump(queryResult))
	if err != nil {
		return err
	}
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	gomega.Expect(queryResult.Volumes[0].Metadata).NotTo(gomega.BeNil())
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		if metadata == nil {
			continue
		}
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		k8sEntityName := kubernetesMetadata.EntityName
		if vs.TestInput.ClusterFlavor.GuestCluster {
			k8sEntityName = kubernetesMetadata.CnsEntityMetadata.EntityName
		}
		if kubernetesMetadata.EntityType == entityType && k8sEntityName == entityName &&
			kubernetesMetadata.Namespace == entityNamespace {
			if matchLabels == nil {
				return nil
			}
			labelsMatch := reflect.DeepEqual(GetLabelsMapFromKeyValue(kubernetesMetadata.Labels), matchLabels)
			if vs.TestInput.ClusterFlavor.GuestCluster {
				labelsMatch = reflect.DeepEqual(GetLabelsMapFromKeyValue(kubernetesMetadata.CnsEntityMetadata.Labels),
					matchLabels)
			}
			if labelsMatch {
				return nil
			} else {
				return fmt.Errorf("labels are not updated to %+v for %s %q for volume %s",
					matchLabels, entityType, entityName, volumeID)
			}
		}
	}
	return nil
}

// getLabelsMapFromKeyValue returns map[string]string for given array of
// vim25types.KeyValue.
func GetLabelsMapFromKeyValue(labels []vim25types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// getVmdkPathFromVolumeHandle returns VmdkPath associated with a given volumeHandle
// by running govc command
func GetVmdkPathFromVolumeHandle(vs *config.E2eTestConfig, sshClientConfig *ssh.ClientConfig, masterIp string,
	datastoreName string, volHandle string) string {
	cmd := GovcLoginCmd(vs) + fmt.Sprintf("govc disk.ls -L=true -ds=%s -l  %s", datastoreName, volHandle)
	result, err := sshExec(vs, sshClientConfig, masterIp, cmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	vmdkPath := result.Stdout
	return vmdkPath
}

// sshExec runs a command on the host via ssh.
func sshExec(vs *config.E2eTestConfig,
	sshClientConfig *ssh.ClientConfig, host string, cmd string) (fssh.Result, error) {
	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	result := fssh.Result{Host: host, Cmd: cmd}
	sshClient, err := ssh.Dial("tcp", addr, sshClientConfig)
	if err != nil {
		result.Stdout = ""
		result.Stderr = ""
		result.Code = 0
		return result, err
	}
	defer sshClient.Close()
	sshSession, err := sshClient.NewSession()
	if err != nil {
		result.Stdout = ""
		result.Stderr = ""
		result.Code = 0
		return result, err
	}
	defer sshSession.Close()
	// Run the command.
	code := 0
	var bytesStdout, bytesStderr bytes.Buffer
	sshSession.Stdout, sshSession.Stderr = &bytesStdout, &bytesStderr
	if err = sshSession.Run(cmd); err != nil {
		if exiterr, ok := err.(*ssh.ExitError); ok {
			// If we got an ExitError and the exit code is nonzero, we'll
			// consider the SSH itself successful but cmd failed on the host.
			if code = exiterr.ExitStatus(); code != 0 {
				err = nil
			}
		} else {
			err = fmt.Errorf("failed running `%s` on %s@%s: '%v'", cmd, sshClientConfig.User, host, err)
		}
	}
	result.Stdout = bytesStdout.String()
	result.Stderr = bytesStderr.String()
	result.Code = code
	if bytesStderr.String() != "" {
		err = fmt.Errorf("failed running `%s` on %s@%s: '%v'", cmd, sshClientConfig.User, host, bytesStderr.String())
	}
	framework.Logf("host: %v, command: %v, return code: %v, stdout: %v, stderr: %v",
		host, cmd, code, bytesStdout.String(), bytesStderr.String())
	return result, err
}

// govc login cmd
func GovcLoginCmd(vs *config.E2eTestConfig) string {
	loginCmd := "export GOVC_INSECURE=1;"
	loginCmd += fmt.Sprintf("export GOVC_URL='https://%s:%s@%s:%s';",
		vs.TestInput.Global.User, vs.TestInput.Global.Password,
		vs.TestInput.Global.VCenterHostname, constants.DefaultVcAdminPortNum)
	return loginCmd
}

// verifyDatastoreMatch verify if any of the given dsUrl matches with the datstore url for the volumeid
func VerifyDatastoreMatch(vs *config.E2eTestConfig, volumeID string, dsUrls []string) {
	actualDatastoreUrl := FetchDsUrl4CnsVol(vs, volumeID)
	gomega.Expect(actualDatastoreUrl).Should(gomega.BeElementOf(dsUrls),
		"Volume is not provisioned on any of the given datastores: %s, but on: %s", dsUrls,
		actualDatastoreUrl)
}

// cnsRelocateVolume relocates volume from one datastore to another using CNS relocate volume API
func CnsRelocateVolume(ctx context.Context, vs *config.E2eTestConfig, fcdID string,
	dsRefDest vim25types.ManagedObjectReference,
	waitForRelocateTaskToComplete ...bool) (*object.Task, error) {
	var pandoraSyncWaitTime int
	var err error
	waitForTaskTocomplete := true
	if len(waitForRelocateTaskToComplete) > 0 {
		waitForTaskTocomplete = waitForRelocateTaskToComplete[0]
	}
	if os.Getenv(constants.EnvPandoraSyncWaitTime) != "" {
		pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvPandoraSyncWaitTime))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		pandoraSyncWaitTime = constants.DefaultPandoraSyncWaitTime
	}

	relocateSpec := cnstypes.NewCnsBlockVolumeRelocateSpec(fcdID, dsRefDest)
	var baseCnsVolumeRelocateSpecList []cnstypes.BaseCnsVolumeRelocateSpec
	baseCnsVolumeRelocateSpecList = append(baseCnsVolumeRelocateSpecList, relocateSpec)
	req := cnstypes.CnsRelocateVolume{
		This:          cnsclient.CnsVolumeManagerInstance,
		RelocateSpecs: baseCnsVolumeRelocateSpecList,
	}

	cnsClient, err := cnsclient.NewCnsClient(ctx, vs.VcClient.Client)
	framework.Logf("error: %v", err)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	res, err := cnsmethods.CnsRelocateVolume(ctx, cnsClient, &req)
	framework.Logf("error is: %v", err)
	if err != nil {
		return nil, err
	}
	task := object.NewTask(vs.VcClient.Client, res.Returnval)
	if waitForTaskTocomplete {
		taskInfo, err := task.WaitForResultEx(ctx, nil)
		framework.Logf("taskInfo: %v", taskInfo)
		framework.Logf("error: %v", err)
		if err != nil {
			return nil, err
		}
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			return nil, err
		}

		framework.Logf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime)
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		cnsRelocateVolumeRes := taskResult.GetCnsVolumeOperationResult()

		if cnsRelocateVolumeRes.Fault != nil {
			err = fmt.Errorf("failed to relocate volume=%+v", cnsRelocateVolumeRes.Fault)
			return nil, err
		}
	}
	return task, nil
}

// fetchDsUrl4CnsVol executes query CNS volume to get the datastore
// where the volume is Present
func FetchDsUrl4CnsVol(vs *config.E2eTestConfig, volHandle string) string {
	framework.Logf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle)
	queryResult, err := QueryCNSVolumeWithResult(vs, volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	return queryResult.Volumes[0].DatastoreUrl
}

// verifyPreferredDatastoreMatch verify if any of the given dsUrl matches with the datstore url for the volumeid
func VerifyPreferredDatastoreMatch(vs *config.E2eTestConfig, volumeID string, dsUrls []string) bool {
	actualDatastoreUrl := FetchDsUrl4CnsVol(vs, volumeID)
	flag := false
	for _, dsUrl := range dsUrls {
		if actualDatastoreUrl == dsUrl {
			flag = true
			return flag
		}
	}
	return flag
}

// Delete CNS volume
func DeleteCNSvolume(vs *config.E2eTestConfig,
	volumeID string, isDeleteDisk bool) (*cnstypes.CnsDeleteVolumeResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Connect to VC
	connections.ConnectToVC(ctx, vs)

	var volumeIds []cnstypes.CnsVolumeId
	volumeIds = append(volumeIds, cnstypes.CnsVolumeId{
		Id: volumeID,
	})

	req := cnstypes.CnsDeleteVolume{
		This:       cnsclient.CnsVolumeManagerInstance,
		VolumeIds:  volumeIds,
		DeleteDisk: isDeleteDisk,
	}

	err := connections.ConnectCns(ctx, vs)
	if err != nil {
		return nil, err
	}
	res, err := cnsmethods.CnsDeleteVolume(ctx, vs.CnsClient.Client, &req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// reconfigPolicy reconfigures given policy on the given volume
func ReconfigPolicy(ctx context.Context, vs *config.E2eTestConfig, volumeID string, profileID string) error {
	cnsClient, err := cnsclient.NewCnsClient(ctx, vs.VcClient.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	CnsVolumeManagerInstance := vim25types.ManagedObjectReference{
		Type:  "CnsVolumeManager",
		Value: "cns-volume-manager",
	}
	req := cnstypes.CnsReconfigVolumePolicy{
		This: CnsVolumeManagerInstance,
		VolumePolicyReconfigSpecs: []cnstypes.CnsVolumePolicyReconfigSpec{
			{
				VolumeId: cnstypes.CnsVolumeId{Id: volumeID},
				Profile: []vim25types.BaseVirtualMachineProfileSpec{
					&vim25types.VirtualMachineDefinedProfileSpec{
						ProfileId: profileID,
					},
				},
			},
		},
	}
	res, err := cnsmethods.CnsReconfigVolumePolicy(ctx, cnsClient, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(vs.VcClient.Client, res.Returnval)
	taskInfo, err := cns.GetTaskInfo(ctx, task)
	if err != nil {
		return err
	}
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	if err != nil {
		return err
	}
	if taskResult == nil {
		return errors.New("TaskInfo result is empty")
	}
	reconfigVolumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if reconfigVolumeOperationRes == nil {
		return errors.New("cnsreconfigpolicy operation result is empty")
	}
	if reconfigVolumeOperationRes.Fault != nil {
		return errors.New("cnsreconfigpolicy operation fault: " + reconfigVolumeOperationRes.Fault.LocalizedMessage)
	}
	framework.Logf("reconfigpolicy on volume %v with policy %v is successful", volumeID, profileID)
	return nil
}

// cnsRelocateVolumeInParallel relocates volume in parallel from one datastore to another
// using CNS API
func CnsRelocateVolumeInParallel(ctx context.Context, vs *config.E2eTestConfig, fcdID string,
	dsRefDest vim25types.ManagedObjectReference, waitForRelocateTaskToComplete bool,
	wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	_, err := CnsRelocateVolume(ctx, vs, fcdID, dsRefDest, waitForRelocateTaskToComplete)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

}

// waitForCNSTaskToComplete wait for CNS task to complete
// and gets the result and checks if any fault has occurred
func WaitForCNSTaskToComplete(ctx context.Context, task *object.Task) *vim25types.LocalizedMethodFault {
	var pandoraSyncWaitTime int
	var err error
	if os.Getenv(constants.EnvPandoraSyncWaitTime) != "" {
		pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvPandoraSyncWaitTime))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		pandoraSyncWaitTime = constants.DefaultPandoraSyncWaitTime
	}

	taskInfo, err := task.WaitForResultEx(ctx, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	taskResult, err := cns.GetTaskResult(ctx, taskInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Sleeping for %v seconds to allow CNS to sync with pandora", pandoraSyncWaitTime)
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	cnsTaskRes := taskResult.GetCnsVolumeOperationResult()
	return cnsTaskRes.Fault
}

func SvmotionVM2DiffDs(ctx context.Context,
	vs *config.E2eTestConfig, vm *object.VirtualMachine, destinationDsUrl string) {
	dsMo := GetDsByUrl(ctx, vs, destinationDsUrl)
	relocateSpec := vim25types.VirtualMachineRelocateSpec{}
	dsref := dsMo.Reference()
	relocateSpec.Datastore = &dsref
	vmname, err := vm.ObjectName(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Starting relocation of vm %s to datastore %s", vmname, dsref.Value)
	task, err := vm.Relocate(ctx, relocateSpec, vim25types.VirtualMachineMovePriorityHighPriority)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = task.WaitForResultEx(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Relocation of vm %s to datastore %s completed successfully", vmname, dsref.Value)
}

func GetDsByUrl(ctx context.Context, vs *config.E2eTestConfig, datastoreURL string) mo.Datastore {
	finder := find.NewFinder(vs.VcClient.Client, false)
	dcString := vs.TestInput.Global.Datacenters
	dc, err := finder.Datacenter(ctx, dcString)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	datastores, err := finder.DatastoreList(ctx, "*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var dsList []vim25types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}
	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(vs.VcClient.Client)
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var dsMo mo.Datastore
	for _, mo := range dsMoList {
		if mo.Info.GetDatastoreInfo().Url == datastoreURL {
			dsMo = mo
		}
	}
	gomega.Expect(dsMo).NotTo(gomega.BeNil())
	return dsMo
}

func GetAllVms(ctx context.Context, vs *config.E2eTestConfig) []*object.VirtualMachine {
	finder := find.NewFinder(vs.VcClient.Client, false)
	dcString := vs.TestInput.Global.Datacenters
	dc, err := finder.Datacenter(ctx, dcString)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	vms, err := finder.VirtualMachineList(ctx, "*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return vms
}

func GenerateEncryptionKey(ctx context.Context, vs *config.E2eTestConfig, keyProviderID string) string {
	vimClient := vs.VcClient.Client
	cm := vimClient.ServiceContent.CryptoManager
	resp, err := methods.GenerateKey(ctx, vimClient, &vim25types.GenerateKey{
		This: *cm,
		KeyProvider: &vim25types.KeyProviderId{
			Id: keyProviderID,
		},
	})

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.Returnval.Success).To(gomega.BeTrue())
	gomega.Expect(resp.Returnval.KeyId.KeyId).NotTo(gomega.BeEmpty())

	return resp.Returnval.KeyId.KeyId
}

func FindKeyProvier(ctx context.Context, vs *config.E2eTestConfig,
	keyProviderID string) (*vim25types.KmipClusterInfo, error) {
	vimClient := vs.VcClient.Client
	cm := vimClient.ServiceContent.CryptoManager
	resp, err := methods.ListKmipServers(ctx, vimClient, &vim25types.ListKmipServers{
		This: *cm,
	})
	if err != nil {
		return nil, err
	}

	for idx := range resp.Returnval {
		kms := &resp.Returnval[idx]
		if kms.ClusterId.Id == keyProviderID {
			return kms, nil
		}
	}

	return nil, nil
}

// getAggregatedSnapshotCapacityInMb - get the cnsvolumeinfo aggregated value of snapshot
func GetAggregatedSnapshotCapacityInMb(vs *config.E2eTestConfig, volHandle string) int64 {
	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := QueryCNSVolumeWithResult(vs, volHandle)
	ginkgo.By(fmt.Sprintf(" queryResult :%v", queryResult.Volumes[0]))

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	ginkgo.By(fmt.Sprintf("volume Name:%s , aggregatorValue:%v",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).AggregatedSnapshotCapacityInMb))

	return queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).AggregatedSnapshotCapacityInMb
}

// renameDs renames a datastore to the given name
func RenameDs(ctx context.Context, vs *config.E2eTestConfig, datastoreName string,
	dsRef *vim25types.ManagedObjectReference) {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	randomStr := strconv.Itoa(r1.Intn(1000))
	req := vim25types.RenameDatastore{
		This:    *dsRef,
		NewName: datastoreName + randomStr,
	}

	_, err := methods.RenameDatastore(ctx, vs.VcClient.Client, &req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// fetchDatastoreNameFromDatastoreUrl fetches datastore name and datastore reference
// from a given datastore url by querying volumeID and a list of datastore present in vCenter
func FetchDatastoreNameFromDatastoreUrl(ctx context.Context, vs *config.E2eTestConfig,
	volumeID string) (string, vim25types.ManagedObjectReference, error) {

	dsUrl := FetchDsUrl4CnsVol(vs, volumeID)
	datastoreName := ""
	var dsRef vim25types.ManagedObjectReference
	finder := find.NewFinder(vs.VcClient.Client, false)
	dcString := vs.TestInput.Global.Datacenters
	dc, err := finder.Datacenter(ctx, dcString)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	datastores, err := finder.DatastoreList(ctx, "*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var dsList []vim25types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}
	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(vs.VcClient.Client)
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, mo := range dsMoList {
		if mo.Info.GetDatastoreInfo().Url == dsUrl {
			dsRef = mo.Reference()
			datastoreName = mo.Info.GetDatastoreInfo().Name
			break
		}
	}

	if datastoreName == "" {
		return "", dsRef, fmt.Errorf("failed to find datastoreName with datastore url: %s", dsUrl)
	}

	return datastoreName, dsRef, nil
}

// renameDsInParallel renames a datastore to the given name in parallel
func RenameDsInParallel(ctx context.Context, vs *config.E2eTestConfig, datastoreName string,
	dsRef *vim25types.ManagedObjectReference, wg *sync.WaitGroup) {
	defer wg.Done()
	RenameDs(ctx, vs, datastoreName, dsRef)
}

/*

*******************These functions moved from util.go as these are vc related functions
 */

// getVMUUIDFromNodeName returns the vmUUID for a given node vm and datacenter.
func GetVMUUIDFromNodeName(vs *config.E2eTestConfig, nodeName string) (string, error) {
	var datacenters []string
	var err error
	finder := find.NewFinder(vs.VcClient.Client, false)
	dcList := strings.Split(vs.TestInput.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}
	var vm *object.VirtualMachine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, dc := range datacenters {
		dataCenter, err := finder.Datacenter(ctx, dc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		finder := find.NewFinder(dataCenter.Client(), false)
		finder.SetDatacenter(dataCenter)
		vm, err = finder.VirtualMachine(ctx, nodeName)
		if err != nil {
			continue
		}
		vmUUID := vm.UUID(ctx)
		gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("VM UUID is: %s for node: %s", vmUUID, nodeName))
		return vmUUID, nil
	}
	return "", err
}

// waitForVolumeDetachedFromNode checks volume is detached from the node
// This function checks disks status every 3 seconds until detachTimeout, which is set to 360 seconds
func WaitForVolumeDetachedFromNode(vs *config.E2eTestConfig, client clientset.Interface,
	volumeID string, nodeName string) (bool, error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if vs.TestInput.ClusterFlavor.SupervisorCluster {
		_, err := GetVMByUUIDWithWait(ctx, vs, nodeName, constants.SupervisorClusterOperationsTimeout)
		if err == nil {
			return false, fmt.Errorf(
				"PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM", nodeName, volumeID)
		} else if strings.Contains(err.Error(), "is not found") {
			return true, nil
		}
		return false, err
	}
	err := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			var vmUUID string
			if vs.TestInput.ClusterFlavor.VanillaCluster {
				vmUUID = GetNodeUUID(ctx, client, nodeName)
			} else {
				vmUUID, _ = GetVMUUIDFromNodeName(vs, nodeName)
			}
			diskAttached, err := IsVolumeAttachedToVM(client, vs, volumeID, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !diskAttached {
				framework.Logf("Disk: %s successfully detached", volumeID)
				return true, nil
			}
			framework.Logf("Waiting for disk: %q to be detached from the node :%q", volumeID, nodeName)
			return false, nil
		})
	if err != nil {
		return false, nil
	}
	return true, nil
}

// getNodeUUID returns Node VM UUID for requested node.
func GetNodeUUID(ctx context.Context, client clientset.Interface, nodeName string) string {
	vmUUID := ""
	csiNode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	csiDriverFound := false
	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == constants.E2evSphereCSIDriverName {
			csiDriverFound = true
			vmUUID = driver.NodeID
		}
	}
	gomega.Expect(csiDriverFound).To(gomega.BeTrue(), "CSI driver not found in CSI node %s", nodeName)
	ginkgo.By(fmt.Sprintf("VM UUID is: %s for node: %s", vmUUID, nodeName))
	return vmUUID
}

// verifyVolumeMetadataInCNS verifies container volume metadata is matching the
// one is CNS cache.
func VerifyVolumeMetadataInCNS(volumeID string, vs *config.E2eTestConfig,
	PersistentVolumeClaimName string, PersistentVolumeName string,
	PodName string, Labels ...vim25types.KeyValue) error {
	queryResult, err := QueryCNSVolumeWithResult(vs, volumeID)
	if err != nil {
		return err
	}
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		if kubernetesMetadata.EntityType == "POD" && kubernetesMetadata.EntityName != PodName {
			return fmt.Errorf("entity Pod with name %s not found for volume %s", PodName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME" &&
			kubernetesMetadata.EntityName != PersistentVolumeName {
			return fmt.Errorf("entity PV with name %s not found for volume %s", PersistentVolumeName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME_CLAIM" &&
			kubernetesMetadata.EntityName != PersistentVolumeClaimName {
			return fmt.Errorf("entity PVC with name %s not found for volume %s", PersistentVolumeClaimName, volumeID)
		}
	}
	labelMap := make(map[string]string)
	for _, e := range queryResult.Volumes[0].Metadata.EntityMetadata {
		if e == nil {
			continue
		}
		if e.GetCnsEntityMetadata().Labels == nil {
			continue
		}
		for _, al := range e.GetCnsEntityMetadata().Labels {
			// These are the actual labels in the provisioned PV. Populate them
			// in the label map.
			labelMap[al.Key] = al.Value
		}
		for _, el := range Labels {
			// Traverse through the slice of expected labels and see if all of them
			// are present in the label map.
			if val, ok := labelMap[el.Key]; ok {
				gomega.Expect(el.Value == val).To(gomega.BeTrue(),
					fmt.Sprintf("Actual label Value of the statically provisioned PV is %s but expected is %s",
						val, el.Value))
			} else {
				return fmt.Errorf("label(%s:%s) is expected in the provisioned PV but its not found", el.Key, el.Value)
			}
		}
	}
	ginkgo.By(fmt.Sprintf("successfully verified metadata of the volume %q", volumeID))
	return nil
}

// writeToFile will take two parameters:
// 1. the absolute path of the file(including filename) to be created.
// 2. data content to be written into the file.
// Returns nil on Success and error on failure.
func WriteToFile(filePath, data string) error {
	if filePath == "" {
		return fmt.Errorf("invalid filename")
	}
	f, err := os.Create(filePath)
	if err != nil {
		framework.Logf("Error: %v", err)
		return err
	}
	_, err = f.WriteString(data)
	if err != nil {
		framework.Logf("Error: %v", err)
		f.Close()
		return err
	}
	err = f.Close()
	if err != nil {
		framework.Logf("Error: %v", err)
		return err
	}
	return nil
}

// This function invokes `dir-cli password reset` command on the
// given vCenter host over SSH, thereby resetting the currentPassword of the
// `user` to the `newPassword`.
func InvokeVCenterChangePassword(ctx context.Context,
	vs *config.E2eTestConfig, user, adminPassword, newPassword,
	host string, clientIndex int) error {
	var copyCmd string
	var removeCmd string
	// Create an input file and write passwords into it.
	path := "input.txt"
	input := fmt.Sprintf("%s\n%s\n", adminPassword, newPassword)
	err := WriteToFile(path, input)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		// Delete the input file containing passwords.
		err = os.Remove(path)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	// Remote copy this input file to VC.
	if !vs.TestInput.TestBedInfo.Multivc {
		copyCmd = fmt.Sprintf("/bin/cat %s | /usr/bin/ssh root@%s '/usr/bin/cat >> input_copy.txt'",
			path, vs.TestInput.Global.VCenterHostname)
	} else {
		vCenter := strings.Split(vs.TestInput.TestBedInfo.VcAddress, ",")[clientIndex]
		copyCmd = fmt.Sprintf("/bin/cat %s | /usr/bin/ssh root@%s '/usr/bin/cat >> input_copy.txt'",
			path, vCenter)
	}
	fmt.Printf("Executing the command: %s\n", copyCmd)
	_, err = exec.Command("/bin/sh", "-c", copyCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		// Remove the input_copy.txt file from VC.
		if !vs.TestInput.TestBedInfo.Multivc {
			removeCmd = fmt.Sprintf("/usr/bin/ssh root@%s '/usr/bin/rm input_copy.txt'",
				vs.TestInput.TestBedInfo.VcAddress)
		} else {
			vCenter := strings.Split(vs.TestInput.TestBedInfo.VcAddress, ",")[clientIndex]
			removeCmd = fmt.Sprintf("/usr/bin/ssh root@%s '/usr/bin/rm input_copy.txt'",
				vCenter)
		}
		_, err = exec.Command("/bin/sh", "-c", removeCmd).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	sshCmd :=
		fmt.Sprintf("/usr/bin/cat input_copy.txt | /usr/lib/vmware-vmafd/bin/dir-cli password reset --account %s", user)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
	result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v, err: %v", sshCmd, addr, err)
	}
	if !strings.Contains(result.Stdout, "Password was reset successfully for ") {
		framework.Logf("failed to change the password for user %s: %s", user, result.Stdout)
		return err
	}
	framework.Logf("password changed successfully for user: %s", user)
	return nil
}

/*
Restart WCP with WaitGroup
*/
func RestartWcpWithWg(ctx context.Context, vs *config.E2eTestConfig, vcAddress string, wg *sync.WaitGroup) {
	defer wg.Done()
	err := RestartWcp(ctx, vs, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

/*
Restart WCP
*/
func RestartWcp(ctx context.Context, vs *config.E2eTestConfig, vcAddress string) error {
	err := InvokeVCenterServiceControl(&vs.TestInput.TestBedInfo, ctx,
		constants.RestartOperation, constants.WcpServiceName, vs.TestInput.TestBedInfo.VcAddress)
	if err != nil {
		return fmt.Errorf("couldn't restart WCP: %v", err)
	}
	return nil
}

// This function starts given service
// and waits for all VPs to come online
func StartVCServiceWait4VPs(ctx context.Context,
	vs *config.E2eTestConfig, vcAddress string,
	service string, isSvcStopped *bool) {
	err := InvokeVCenterServiceControl(&vs.TestInput.TestBedInfo,
		ctx, constants.StartOperation, service, vs.TestInput.TestBedInfo.VcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = WaitVCenterServiceToBeInState(ctx, vs, service, vcAddress, constants.SvcRunningMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*isSvcStopped = false
}

// invokeVCenterServiceControl invokes the given command for the given service
// via service-control on the given vCenter host over SSH.
func InvokeVCenterServiceControl(testConfig *config.TestBedConfig,
	ctx context.Context, command, service, host string) error {
	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(testConfig, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	sshCmd := fmt.Sprintf("service-control --%s %s", command, service)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
	result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", sshCmd, addr, err)
	}
	return nil
}

// checkVcServicesHealthPostReboot returns waitErr, if VC services are not running in given timeout
func CheckVcServicesHealthPostReboot(ctx context.Context,
	vs *config.E2eTestConfig, host string, timeout ...time.Duration) error {
	var pollTime time.Duration
	// if timeout is not passed then default pollTime to be set to 30 mins
	if len(timeout) == 0 {
		pollTime = constants.PollTimeout * 6
	} else {
		pollTime = timeout[0]
	}
	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	//list of default stopped services in VC
	var defaultStoppedServicesList = []string{"vmcam", "vmware-imagebuilder", "vmware-netdumper",
		"vmware-rbd-watchdog", "vmware-vcha"}
	waitErr := wait.PollUntilContextTimeout(ctx, constants.PollTimeoutShort, pollTime, true,
		func(ctx context.Context) (bool, error) {
			var pendingServiceslist []string
			var noAdditionalServiceStopped = false
			sshCmd := fmt.Sprintf("service-control --%s", constants.StatusOperation)
			framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
			result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
			}
			framework.Logf("Command %v output is %v", sshCmd, result.Stdout)
			// getting list of services which are stopped currently
			services := strings.SplitAfter(result.Stdout, constants.SvcRunningMessage+":")
			servicesStoppedInVc := strings.Split(services[1], constants.SvcStoppedMessage+":")
			// getting list if pending services if any
			if strings.Contains(servicesStoppedInVc[0], "StartPending:") {
				pendingServiceslist = strings.Split(servicesStoppedInVc[0], "StartPending:")
				pendingServiceslist = strings.Split(strings.Trim(pendingServiceslist[1], "\n "), " ")
				framework.Logf("Additional service %s in StartPending state", pendingServiceslist)
			}
			servicesStoppedInVc = strings.Split(strings.Trim(servicesStoppedInVc[1], "\n "), " ")
			// checking if current stopped services are same as defined above
			if reflect.DeepEqual(servicesStoppedInVc, defaultStoppedServicesList) {
				framework.Logf("All required vCenter services are in up and running state")
				noAdditionalServiceStopped = true
				// wait for 1 min,in case dependent service are still in pending state
				time.Sleep(1 * time.Minute)
			} else {
				for _, service := range servicesStoppedInVc {
					if !(slices.Contains(defaultStoppedServicesList, service)) {
						framework.Logf("Starting additional service %s in stopped state", service)
						_ = InvokeVCenterServiceControl(&vs.TestInput.TestBedInfo, ctx, constants.StartOperation, service, host)
						// wait for 120 seconds,in case dependent service are still in pending state
						time.Sleep(120 * time.Second)
					}
				}
			}
			// Checking status for pendingStart Service list and starting it accordingly
			for _, service := range pendingServiceslist {
				framework.Logf("Checking status for additional service %s in StartPending state", service)
				sshCmd := fmt.Sprintf("service-control --%s %s", constants.StatusOperation, service)
				framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
				result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
				if err != nil || result.Code != 0 {
					fssh.LogResult(result)
					return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
				}
				if strings.Contains(strings.TrimSpace(result.Stdout), "Running") {
					framework.Logf("Additional service %s got into Running from StartPending state", service)
				} else {
					framework.Logf("Starting additional service %s in StartPending state", service)
					err = InvokeVCenterServiceControl(&vs.TestInput.TestBedInfo,
						ctx, constants.StartOperation, service, host)
					if err != nil {
						return false, fmt.Errorf("couldn't start service : %s on vCenter host: %v", service, err)
					}
					// wait for 30 seconds,in case dependent service are still in pending state
					time.Sleep(30 * time.Second)
				}
			}
			if noAdditionalServiceStopped && len(pendingServiceslist) == 0 {
				return true, nil
			}
			return false, nil
		})
	return waitErr

}

// waitVCenterServiceToBeInState invokes the status check for the given service and waits
// via service-control on the given vCenter host over SSH.
func WaitVCenterServiceToBeInState(ctx context.Context,
	vs *config.E2eTestConfig, serviceName string, host string, state string) error {

	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeoutShort*2, true,
		func(ctx context.Context) (bool, error) {
			sshCmd := fmt.Sprintf("service-control --%s %s", "status", serviceName)
			framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
			result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)

			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
			}
			if strings.Contains(result.Stdout, state) {
				fssh.LogResult(result)
				framework.Logf("Found service %v in %v state", serviceName, state)
				return true, nil
			}
			framework.Logf("Command %v output is %v", sshCmd, result.Stdout)
			return false, nil
		})
	return waitErr
}

// This function checks and polls
// for vCenter essential services status
// to be in running state
func CheckVcenterServicesRunning(
	ctx context.Context, vs *config.E2eTestConfig,
	host string, essentialServices []string, timeout ...time.Duration) {

	var pollTime time.Duration
	if len(timeout) == 0 {
		pollTime = constants.PollTimeout * 6
	} else {
		pollTime = timeout[0]
	}

	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, pollTime, true,
		func(ctx context.Context) (bool, error) {
			var runningServices []string
			var statusMap = make(map[string]bool)
			allServicesRunning := true
			sshCmd := fmt.Sprintf("service-control --%s", constants.StatusOperation)
			framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
			result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
			}
			framework.Logf("Command %v output is %v", sshCmd, result.Stdout)

			services := strings.SplitAfter(result.Stdout, constants.SvcRunningMessage+":")
			stoppedService := strings.Split(services[1], constants.SvcStoppedMessage+":")
			if strings.Contains(stoppedService[0], "StartPending:") {
				runningServices = strings.Split(stoppedService[0], "StartPending:")
			} else {
				runningServices = append(runningServices, stoppedService[0])
			}

			for _, service := range essentialServices {
				if strings.Contains(runningServices[0], service) {
					statusMap[service] = true
					framework.Logf("%s in running state", service)
				} else {
					statusMap[service] = false
					framework.Logf("%s not in running state", service)
				}
			}
			for _, service := range essentialServices {
				if !statusMap[service] {
					allServicesRunning = false
				}
			}
			if allServicesRunning {
				framework.Logf("All services are in running state")
				return true, nil
			}
			return false, nil
		})
	gomega.Expect(waitErr).NotTo(gomega.HaveOccurred())
	// Checking for any extra services which needs to be started if in stopped or pending state after vc reboot
	err = CheckVcServicesHealthPostReboot(ctx, vs, host, timeout...)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(),
		"Got timed-out while waiting for all required VC services to be up and running")
}

// EnterHostIntoMM puts a host into maintenance mode with a particular timeout and
// maintenance mode type
func EnterHostIntoMM(ctx context.Context, host *object.HostSystem, mmModeType string,
	timeout int32, evacuateVms bool) {
	mmSpec := vim25types.VsanHostDecommissionMode{
		ObjectAction: mmModeType,
	}
	hostMMSpec := vim25types.HostMaintenanceSpec{
		VsanMode: &mmSpec,
		Purpose:  "",
	}
	task, err := host.EnterMaintenanceMode(ctx, timeout, false, &hostMMSpec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = task.WaitForResultEx(ctx, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Host: %v in in maintenance mode", host)
}

// ExitHostMM exits a host from maintenance mode with a particular timeout
func ExitHostMM(ctx context.Context, host *object.HostSystem, timeout int32) {
	task, err := host.ExitMaintenanceMode(ctx, timeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = task.WaitForResultEx(ctx, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Host: %v exited from maintenance mode", host)
}

// GetClusterRefFromClusterName gets cluster Moid from given vsphere cluster name
func GetClusterRefFromClusterName(ctx context.Context, vs *config.E2eTestConfig,
	clusterName string) (vim25types.ManagedObjectReference, error) {

	clusterComputeResource, _, err := GetClusterName(ctx, vs)
	if err != nil {
		return vim25types.ManagedObjectReference{}, err
	}

	for _, cluster := range clusterComputeResource {
		if cluster.Name() == clusterName {
			return cluster.Reference(), nil
		}

	}
	return vim25types.ManagedObjectReference{}, nil
}

// QueryVsanFileShares fetches vsan file shares from vsan endpoint by giving file share IDs and domain ID
func QueryVsanFileShares(ctx context.Context, vs *config.E2eTestConfig, fileShareIDs []string,
	clusterRef vim25types.ManagedObjectReference) []vsantypes.VsanFileShare {

	fileShareDomainID := env.GetAndExpectStringEnvVar(constants.EnvFileShareDomainID)
	var (
		VsanQueryObjectIdentitiesInstance = vim25types.ManagedObjectReference{
			Type:  constants.VsanFileServiceType,
			Value: constants.VsanClusterFileServiceSystem,
		}
	)

	querySpec := &vsantypes.VsanFileShareQuerySpec{
		DomainName: fileShareDomainID,
		Uuids:      fileShareIDs,
	}
	spec := &vsantypes.VsanClusterQueryFileShares{
		This:      VsanQueryObjectIdentitiesInstance,
		QuerySpec: *querySpec,
		Cluster:   &clusterRef,
	}
	_, vsanHealthClient := GetClusterComputeResource(ctx, vs)

	vsanClient, err := vsanpackage.NewClient(ctx, vsanHealthClient.Vim25Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vsanFileShares, err := vsanmethods.VsanClusterQueryFileShares(ctx, vsanClient, spec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vsanFileShareList := vsanFileShares.Returnval.FileShares
	return vsanFileShareList
}

// getDsMoRefFromURL get datastore MoRef from its URL
func GetDsMoRefFromURL(ctx context.Context, vs *config.E2eTestConfig, dsURL string) vim25types.ManagedObjectReference {
	dcList, err := GetAllDatacenters(ctx, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var ds *object.Datastore
	for _, dc := range dcList {
		ds, err = GetDatastoreByURL(ctx, vs, dsURL, dc)
		if err != nil {
			if !strings.Contains(err.Error(), "couldn't find Datastore given URL") {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			break
		}
	}
	gomega.Expect(ds).NotTo(gomega.BeNil(), "Could not find MoRef for ds URL %v", dsURL)
	return ds.Reference()
}
