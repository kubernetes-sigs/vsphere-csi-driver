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

package utils

import (
	"context"
	"math"
	"strconv"

	vmoperatorv1alpha1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmoperatorv1alpha2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoperatorv1alpha3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// DefaultQuerySnapshotLimit constant is already present in pkg/csi/service/common/constants.go
	// However, using that constant creates an import cycle.
	// TODO: Refactor to move all the constants into a top level directory.
	DefaultQuerySnapshotLimit = int64(128)

	vmOperatorApiVersionPrefix = "vmoperator.vmware.com"
	virtualMachineCRDName      = "virtualmachines.vmoperator.vmware.com"
)

var getLatestCRDVersion = kubernetes.GetLatestCRDVersion

// ListVirtualMachines lists all the virtual machines
// converted to the latest API version(v1alpha4).
// Since, VM Operator converts all the older API versions to the latest version,
// this function determines the latest API version of the VirtualMachine CRD and lists the resources.
func ListVirtualMachines(ctx context.Context, clt client.Client,
	namespace string) (*vmoperatorv1alpha4.VirtualMachineList, error) {
	log := logger.GetLogger(ctx)

	version, err := getLatestCRDVersion(ctx, virtualMachineCRDName)
	if err != nil {
		log.Errorf("failed to get latest CRD version for %s: %s", virtualMachineCRDName, err)
		return nil, err
	}

	vmList := &vmoperatorv1alpha4.VirtualMachineList{}
	log.Info("Attempting to list virtual machines with the latest API version ", version)
	switch version {
	case "v1alpha1":
		vmAlpha1List := &vmoperatorv1alpha1.VirtualMachineList{}
		err := clt.List(ctx, vmAlpha1List, client.InNamespace(namespace))
		if err != nil {
			log.Error("failed listing virtual machines for v1alpha1: ", err)
			return nil, err
		}

		err = vmoperatorv1alpha1.Convert_v1alpha1_VirtualMachineList_To_v1alpha4_VirtualMachineList(
			vmAlpha1List, vmList, nil)
		if err != nil {
			log.Fatal("Error converting v1alpha1 virtual machines to v1alpha4: ", err)
			return nil, err
		}
	case "v1alpha2":
		vmAlpha2List := &vmoperatorv1alpha2.VirtualMachineList{}
		err := clt.List(ctx, vmAlpha2List, client.InNamespace(namespace))
		if err != nil {
			log.Error("failed listing virtual machines for v1alpha2: ", err)
			return nil, err
		}

		err = vmoperatorv1alpha2.Convert_v1alpha2_VirtualMachineList_To_v1alpha4_VirtualMachineList(
			vmAlpha2List, vmList, nil)
		if err != nil {
			log.Fatal("Error converting v1alpha2 virtual machines to v1alpha4: ", err)
			return nil, err
		}
	case "v1alpha3":
		vmAlpha3List := &vmoperatorv1alpha3.VirtualMachineList{}
		err := clt.List(ctx, vmAlpha3List, client.InNamespace(namespace))
		if err != nil {
			log.Error("failed listing virtual machines for v1alpha3: ", err)
			return nil, err
		}

		err = vmoperatorv1alpha3.Convert_v1alpha3_VirtualMachineList_To_v1alpha4_VirtualMachineList(
			vmAlpha3List, vmList, nil)
		if err != nil {
			log.Error("Error converting v1alpha3 virtual machines to v1alpha4: ", err)
			return nil, err
		}
	case "v1alpha4":
		vmAlpha4List := &vmoperatorv1alpha4.VirtualMachineList{}
		err := clt.List(context.Background(), vmAlpha4List, client.InNamespace(namespace))
		if err != nil {
			log.Error("failed listing virtual machines for v1alpha4: ", err)
			return nil, err
		}

		// No conversion needed for v1alpha4 as it is the latest version.
		vmList.Items = append(vmList.Items, vmAlpha4List.Items...)
	default:
		// XXX: This should ideally never happen.
		log.Errorf("Unsupported version: %s. Something is fishy...", version)
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"Unsupported version: %s. Something is fishy...", version)
	}

	log.Infof("Successfully listed %d virtual machines in namespace %s",
		len(vmList.Items), namespace)
	return vmList, nil
}

// GetVirtualMachine fetches the virtual machine with the specified key
// converted to the latest API version(v1alpha4).
// Additionally, it returns the API version of the virtual machine CRD.
func GetVirtualMachine(ctx context.Context, clt client.Client,
	vmKey types.NamespacedName) (*vmoperatorv1alpha4.VirtualMachine, string, error) {
	log := logger.GetLogger(ctx)

	version, err := kubernetes.GetLatestCRDVersion(ctx, virtualMachineCRDName)
	if err != nil {
		log.Errorf("failed to get latest CRD version for %s: %s", virtualMachineCRDName, err)
		return nil, "", err
	}

	log.Infof("finding virtual machine with key: %s, version: %s", vmKey.String(), version)
	vm := &vmoperatorv1alpha4.VirtualMachine{}
	apiVersion := vmOperatorApiVersionPrefix + "/" + version
	switch version {
	case "v1alpha1":
		vmV1alpha1 := &vmoperatorv1alpha1.VirtualMachine{}
		err = clt.Get(ctx, vmKey, vmV1alpha1)
		if err != nil {
			log.Errorf("failed to get virtual machine with key %s in v1alpha1: %s", vmKey.String(), err)
			return nil, "", err
		}

		err = vmoperatorv1alpha1.Convert_v1alpha1_VirtualMachine_To_v1alpha4_VirtualMachine(
			vmV1alpha1, vm, nil)
		if err != nil {
			log.Error("Error converting v1alpha1 virtual machine to v1alpha4:", err)
			return nil, "", err
		}
	case "v1alpha2":
		vmV1alpha2 := &vmoperatorv1alpha2.VirtualMachine{}
		err = clt.Get(ctx, vmKey, vmV1alpha2)
		if err != nil {
			log.Errorf("failed to get virtual machine with key %s in v1alpha2: %s", vmKey.String(), err)
			return nil, "", err
		}

		err = vmoperatorv1alpha2.Convert_v1alpha2_VirtualMachine_To_v1alpha4_VirtualMachine(
			vmV1alpha2, vm, nil)
		if err != nil {
			log.Error("Error converting v1alpha2 virtual machine to v1alpha4:", err)
			return nil, "", err
		}
	case "v1alpha3":
		vmV1alpha3 := &vmoperatorv1alpha3.VirtualMachine{}
		err = clt.Get(ctx, vmKey, vmV1alpha3)
		if err != nil {
			log.Errorf("failed to get virtual machine with key %s in v1alpha3: %s", vmKey.String(), err)
			return nil, "", err
		}

		err = vmoperatorv1alpha3.Convert_v1alpha3_VirtualMachine_To_v1alpha4_VirtualMachine(
			vmV1alpha3, vm, nil)
		if err != nil {
			log.Error("Error converting v1alpha3 virtual machine to v1alpha4:", err)
			return nil, "", err
		}
	case "v1alpha4":
		err = clt.Get(ctx, vmKey, vm)
		if err != nil {
			log.Errorf("failed to get virtual machine with key %s in v1alpha4: %s", vmKey.String(), err)
			return nil, "", err
		}
	default:
		log.Errorf("Unsupported version: %s. Something is fishy...", version)
		return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
			"Unsupported version: %s. Something is fishy...", version)
	}

	return vm, apiVersion, nil
}

// UpdateVirtualMachine updates the virtual machine after converting it to the latest API version.
func UpdateVirtualMachine(ctx context.Context, clt client.Client,
	vmV1alpha4 *vmoperatorv1alpha4.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	version, err := kubernetes.GetLatestCRDVersion(ctx, virtualMachineCRDName)
	if err != nil {
		log.Errorf("failed to get latest CRD version for %s: %s", virtualMachineCRDName, err)
		return err
	}

	log.Infof("Updating virtual machine with key: %s, version: %s", vmV1alpha4.Name, version)
	switch version {
	case "v1alpha1":
		vmV1alpha1 := &vmoperatorv1alpha1.VirtualMachine{}
		err = vmoperatorv1alpha1.Convert_v1alpha4_VirtualMachine_To_v1alpha1_VirtualMachine(
			vmV1alpha4, vmV1alpha1, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha1:", err)
			return err
		}

		err = clt.Update(ctx, vmV1alpha1)
		if err != nil {
			log.Errorf("failed to update virtual machine with key %s in v1alpha1: %s", vmV1alpha4.Name, err)
			return err
		}

	case "v1alpha2":
		vmV1alpha2 := &vmoperatorv1alpha2.VirtualMachine{}
		err = vmoperatorv1alpha2.Convert_v1alpha4_VirtualMachine_To_v1alpha2_VirtualMachine(
			vmV1alpha4, vmV1alpha2, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha2:", err)
			return err
		}

		err = clt.Update(ctx, vmV1alpha2)
		if err != nil {
			log.Errorf("failed to update virtual machine with key %s in v1alpha2: %s", vmV1alpha4.Name, err)
			return err
		}
	case "v1alpha3":
		vmV1alpha3 := &vmoperatorv1alpha3.VirtualMachine{}
		err = vmoperatorv1alpha3.Convert_v1alpha4_VirtualMachine_To_v1alpha3_VirtualMachine(
			vmV1alpha4, vmV1alpha3, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha3:", err)
			return err
		}

		err = clt.Update(ctx, vmV1alpha3)
		if err != nil {
			log.Errorf("failed to update virtual machine with key %s in v1alpha3: %s", vmV1alpha4.Name, err)
			return err
		}
	case "v1alpha4":
		err = clt.Update(ctx, vmV1alpha4)
		if err != nil {
			log.Errorf("failed to update virtual machine with key %s in v1alpha4: %s", vmV1alpha4.Name, err)
		}
	default:
		log.Errorf("Unsupported version: %s. Something is fishy...", version)
		return logger.LogNewErrorCodef(log, codes.Internal,
			"Unsupported version: %s. Something is fishy...", version)
	}

	return nil
}

// PatchVirtualMachine patches the virtual machine with the new state
// after converting it to the latest API version.
func PatchVirtualMachine(ctx context.Context, clt client.Client,
	newVM, oldVM *vmoperatorv1alpha4.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	version, err := kubernetes.GetLatestCRDVersion(ctx, virtualMachineCRDName)
	if err != nil {
		log.Errorf("failed to get latest CRD version for %s: %s", virtualMachineCRDName, err)
		return err
	}

	log.Infof("Patching virtual machine with key: %s, version: %s", newVM.Name, version)
	switch version {
	case "v1alpha1":
		newV1Alpha1VM, oldV1Alpha1VM := &vmoperatorv1alpha1.VirtualMachine{}, &vmoperatorv1alpha1.VirtualMachine{}
		err := vmoperatorv1alpha1.Convert_v1alpha4_VirtualMachine_To_v1alpha1_VirtualMachine(
			newVM, newV1Alpha1VM, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha1:", err)
			return err
		}

		err = vmoperatorv1alpha1.Convert_v1alpha4_VirtualMachine_To_v1alpha1_VirtualMachine(
			oldVM, oldV1Alpha1VM, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha1:", err)
			return err
		}

		patch := client.MergeFromWithOptions(
			oldV1Alpha1VM.DeepCopy(),
			client.MergeFromWithOptimisticLock{})
		err = clt.Patch(ctx, newV1Alpha1VM, patch)
		if err != nil {
			log.Errorf("failed to patch virtual machine with key %s in v1alpha1: %s", newVM.Name, err)
			return err
		}
	case "v1alpha2":
		newV1Alpha2VM, oldV1Alpha2VM := &vmoperatorv1alpha2.VirtualMachine{}, &vmoperatorv1alpha2.VirtualMachine{}
		err = vmoperatorv1alpha2.Convert_v1alpha4_VirtualMachine_To_v1alpha2_VirtualMachine(
			newVM, newV1Alpha2VM, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha2:", err)
			return err
		}

		err = vmoperatorv1alpha2.Convert_v1alpha4_VirtualMachine_To_v1alpha2_VirtualMachine(
			oldVM, oldV1Alpha2VM, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha2:", err)
			return err
		}

		patch := client.MergeFromWithOptions(
			oldV1Alpha2VM.DeepCopy(),
			client.MergeFromWithOptimisticLock{})
		err = clt.Patch(ctx, newV1Alpha2VM, patch)
		if err != nil {
			log.Errorf("failed to patch virtual machine with key %s in v1alpha2: %s", newVM.Name, err)
			return err
		}
	case "v1alpha3":
		newV1Alpha3VM, oldV1Alpha3VM := &vmoperatorv1alpha3.VirtualMachine{}, &vmoperatorv1alpha3.VirtualMachine{}
		err = vmoperatorv1alpha3.Convert_v1alpha4_VirtualMachine_To_v1alpha3_VirtualMachine(
			newVM, newV1Alpha3VM, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha3:", err)
			return err
		}

		err = vmoperatorv1alpha3.Convert_v1alpha4_VirtualMachine_To_v1alpha3_VirtualMachine(
			oldVM, oldV1Alpha3VM, nil)
		if err != nil {
			log.Error("Error converting v1alpha4 virtual machine to v1alpha3:", err)
			return err
		}

		patch := client.MergeFromWithOptions(
			oldV1Alpha3VM.DeepCopy(),
			client.MergeFromWithOptimisticLock{})
		err = clt.Patch(ctx, newV1Alpha3VM, patch)
		if err != nil {
			log.Errorf("failed to patch virtual machine with key %s in v1alpha3: %s", newVM.Name, err)
			return err
		}
	case "v1alpha4":
		patch := client.MergeFromWithOptions(
			oldVM.DeepCopy(),
			client.MergeFromWithOptimisticLock{})
		err = clt.Patch(ctx, newVM, patch)
		if err != nil {
			log.Errorf("failed to patch virtual machine with key %s in v1alpha4: %s", newVM.Name, err)
			return err
		}
	default:
		log.Errorf("Unsupported version: %s. Something is fishy...", version)
		return logger.LogNewErrorCodef(log, codes.Internal,
			"Unsupported version: %s. Something is fishy...", version)
	}

	return nil
}

// QueryVolumeUtil helps to invoke query volume API based on the feature
// state set for using query async volume. If useQueryVolumeAsync is set to
// true, the function invokes CNS QueryVolumeAsync, otherwise it invokes
// synchronous QueryVolume API. The function also take volume manager instance,
// query filters, query selection as params. Returns queryResult when query
// volume succeeds, otherwise returns appropriate errors.
func QueryVolumeUtil(ctx context.Context, m cnsvolume.Manager, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection, useQueryVolumeAsync bool) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	var queryAsyncNotSupported bool
	var queryResult *cnstypes.CnsQueryResult
	var err error
	if useQueryVolumeAsync {
		// AsyncQueryVolume feature switch is enabled.
		if queryFilter.Cursor != nil {
			log.Debugf("Calling QueryVolumeAsync with queryFilter limit: %v and offset: %v "+
				"for ContainerClusterIds %v", queryFilter.Cursor.Limit,
				queryFilter.Cursor.Offset, queryFilter.ContainerClusterIds)
		}
		queryResult, err = m.QueryVolumeAsync(ctx, queryFilter, querySelection)
		if err != nil {
			if err.Error() == cnsvsphere.ErrNotSupported.Error() {
				log.Warn("QueryVolumeAsync is not supported. Invoking QueryVolume API")
				queryAsyncNotSupported = true
			} else { // Return for any other failures.
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"queryVolumeAsync failed for queryFilter cursor: %+v and ContainerClusterIds: %+v "+
						"Err=%+v", queryFilter.Cursor, queryFilter.ContainerClusterIds, err.Error())
			}
		}
	}
	if !useQueryVolumeAsync || queryAsyncNotSupported {
		queryResult, err = m.QueryVolume(ctx, queryFilter)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"queryVolume failed for queryFilter: %+v. Err=%+v", queryFilter, err.Error())
		}
	}
	return queryResult, nil
}

// QuerySnapshotsUtil helps invoke CNS QuerySnapshot API. The method takes in a snapshotQueryFilter that represents
// the criteria to retrieve the snapshots. The maxEntries represents the max number of results that the caller of this
// method can handle.
func QuerySnapshotsUtil(ctx context.Context, m cnsvolume.Manager, snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter,
	maxEntries int64) ([]cnstypes.CnsSnapshotQueryResultEntry, string, error) {
	log := logger.GetLogger(ctx)
	var allQuerySnapshotResults []cnstypes.CnsSnapshotQueryResultEntry
	var snapshotQuerySpec cnstypes.CnsSnapshotQuerySpec
	var batchSize int64
	maxIteration := int64(1)
	isMaxIterationSet := false
	if snapshotQueryFilter.SnapshotQuerySpecs == nil {
		log.Infof("Attempting to retrieve all the Snapshots available in the vCenter inventory.")
	} else {
		snapshotQuerySpec = snapshotQueryFilter.SnapshotQuerySpecs[0]
		log.Infof("Invoking QuerySnapshots with spec: %+v", snapshotQuerySpec)
	}
	// Check if cursor is specified, if not set a default cursor.
	if snapshotQueryFilter.Cursor == nil {
		// Setting the default limit(128) explicitly.
		snapshotQueryFilter = cnstypes.CnsSnapshotQueryFilter{
			Cursor: &cnstypes.CnsCursor{
				Offset: 0,
				Limit:  DefaultQuerySnapshotLimit,
			},
		}
		batchSize = DefaultQuerySnapshotLimit
	} else {
		batchSize = snapshotQueryFilter.Cursor.Limit
	}
	iteration := int64(1)
	for {
		if iteration > maxIteration {
			// Exceeds the max number of results that can be handled by callers.
			nextToken := strconv.FormatInt(snapshotQueryFilter.Cursor.Offset, 10)
			log.Infof("the number of results: %d approached max-entries: %d for "+
				"limit: %d in iteration: %d, returning with next-token: %s",
				len(allQuerySnapshotResults), maxEntries, batchSize, iteration, nextToken)
			return allQuerySnapshotResults, nextToken, nil
		}
		log.Infof("invoking QuerySnapshots in iteration: %d with offset: %d and limit: %d, current total "+
			"results: %d", iteration, snapshotQueryFilter.Cursor.Offset, snapshotQueryFilter.Cursor.Limit,
			len(allQuerySnapshotResults))
		snapshotQueryResult, err := m.QuerySnapshots(ctx, snapshotQueryFilter)
		if err != nil {
			log.Errorf("querySnapshots failed for snapshotQueryFilter: %v. Err=%+v", snapshotQueryFilter, err)
			return nil, "", err
		}
		if snapshotQueryResult == nil {
			log.Infof("Observed empty SnapshotQueryResult")
			break
		}
		if len(snapshotQueryResult.Entries) == 0 {
			log.Infof("QuerySnapshots retrieved no results for the spec: %+v", snapshotQuerySpec)
		}
		// Update the max iteration.
		// isMaxIterationSet ensures that the max iterations are set only once, this is to ensure that the number of
		// results are lower than the max entries supported by caller in a busy system which has increasing number
		// total records.
		if !isMaxIterationSet {
			if snapshotQueryResult.Cursor.TotalRecords < maxEntries {
				// If the total number of records is less than max entries supported by caller then
				// all results can be retrieved in a loop, when the results are returned no next-token is expected to be set.
				// Example:
				// maxEntries=200, totalRecords=150, batchSize=128
				// maxIteration=2
				// iteration-1: 128 results, iteration-2: 22 results
				// total results returned: 150
				// offset=0
				maxRecords := snapshotQueryResult.Cursor.TotalRecords
				numOfIterationsForAllResults := float64(maxRecords) / float64(batchSize)
				maxIteration = int64(math.Ceil(numOfIterationsForAllResults))
				log.Infof("setting max iteration to %d for total records count: %d", maxIteration, maxRecords)
			} else {
				// All results cannot be returned to caller, in this case the expectation is return as many results with a
				// nextToken.
				// Example:
				// maxEntries=150, totalRecords=200, batchSize=128
				// maxIteration=1
				// iteration-1: 128 results
				// total results returned: 128
				// offset= 1, callers are expected to call with new offset as next token.
				maxRecords := maxEntries
				numOfIterationsForAllResults := float64(maxRecords) / float64(batchSize)
				maxIteration = int64(math.Floor(numOfIterationsForAllResults))
				log.Infof("setting max iteration to %d for total records count: %d and max limit: %d",
					maxIteration, snapshotQueryResult.Cursor.TotalRecords, maxRecords)
			}
			isMaxIterationSet = true
		}

		allQuerySnapshotResults = append(allQuerySnapshotResults, snapshotQueryResult.Entries...)
		log.Infof("%d more snapshots to be queried",
			snapshotQueryResult.Cursor.TotalRecords-snapshotQueryResult.Cursor.Offset)
		if snapshotQueryResult.Cursor.Offset == snapshotQueryResult.Cursor.TotalRecords {
			log.Infof("QuerySnapshots retrieved all records (%d) for the SnapshotQuerySpec: %+v in %d iterations",
				snapshotQueryResult.Cursor.TotalRecords, snapshotQuerySpec, iteration)
			break
		}
		iteration++
		snapshotQueryFilter.Cursor = &snapshotQueryResult.Cursor
	}
	return allQuerySnapshotResults, "", nil
}

type CnsVolumeDetails struct {
	VolumeID     string
	SizeInMB     int64
	DatastoreUrl string
	VolumeType   string
}

// QueryVolumeDetailsUtil queries Capacity in MB and datastore URL for the source volume with expected volume type.
func QueryVolumeDetailsUtil(ctx context.Context, m cnsvolume.Manager, volumeIds []cnstypes.CnsVolumeId) (
	map[string]*CnsVolumeDetails, error) {
	log := logger.GetLogger(ctx)
	volumeDetailsMap := make(map[string]*CnsVolumeDetails)
	// Select only the backing object details, volume type and datastore.
	querySelection := &cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
			string(cnstypes.QuerySelectionNameTypeVolumeType),
			string(cnstypes.QuerySelectionNameTypeDataStoreUrl),
		},
	}
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
	}
	log.Infof("Invoking QueryAllVolumeUtil with Filter: %+v, Selection: %+v", queryFilter, *querySelection)
	allQueryResults, err := m.QueryAllVolume(ctx, queryFilter, *querySelection)
	if err != nil {
		log.Errorf("failed to retrieve the volume size and datastore, err: %+v", err)
		return volumeDetailsMap, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to retrieve the volume sizes: %+v", err)
	}
	log.Infof("Number of results from QueryAllVolumeUtil: %d", len(allQueryResults.Volumes))
	for _, res := range allQueryResults.Volumes {
		volumeId := res.VolumeId
		datastoreUrl := res.DatastoreUrl
		volumeCapacityInMB := res.BackingObjectDetails.GetCnsBackingObjectDetails().CapacityInMb
		volumeType := res.VolumeType
		log.Debugf("VOLUME: %s, TYPE: %s, DATASTORE: %s, CAPACITY: %d", volumeId, volumeType, datastoreUrl,
			volumeCapacityInMB)
		volumeDetails := &CnsVolumeDetails{
			VolumeID:     volumeId.Id,
			SizeInMB:     volumeCapacityInMB,
			DatastoreUrl: datastoreUrl,
			VolumeType:   volumeType,
		}
		volumeDetailsMap[volumeId.Id] = volumeDetails
	}
	return volumeDetailsMap, nil
}

// LogoutAllvCenterSessions will logout all vCenter sessions and disconnect vCenter client
func LogoutAllvCenterSessions(ctx context.Context) {
	log := logger.GetLogger(ctx)
	log.Info("Logging out all vCenter sessions")
	virtualcentermanager := cnsvsphere.GetVirtualCenterManager(ctx)
	vCenters := virtualcentermanager.GetAllVirtualCenters()
	for _, vc := range vCenters {
		if vc.Client == nil {
			continue
		}
		log.Infof("Disconnecting vCenter client for host %s", vc.Config.Host)
		err := vc.Disconnect(ctx)
		if err != nil {
			log.Errorf("Error while disconnect vCenter client for host %s. Error: %+v", vc.Config.Host, err)
			continue
		}
		log.Infof("Disconnected vCenter client for host %s", vc.Config.Host)
	}
	log.Info("Successfully logged out vCenter sessions")
}

// QueryAllVolumesForCluster API returns QueryResult with all volumes for requested Cluster
func QueryAllVolumesForCluster(ctx context.Context, m cnsvolume.Manager, clusterID string,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	queryFilter := cnstypes.CnsQueryFilter{
		ContainerClusterIds: []string{clusterID},
	}
	queryAllResult, err := m.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"QueryAllVolume failed with err=%+v", err.Error())
	}
	return queryAllResult, nil
}
