/*
Copyright 2020 The Kubernetes Authors.

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

package storagepool

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

const (
	spTypePrefix        = "cns.vmware.com/"
	spTypeLabelKey      = spTypePrefix + "StoragePoolType"
	spTypeAnnotationKey = spTypePrefix + "StoragePoolTypeHint"
)

// getDatastoreProperties returns the total capacity, freeSpace, URL, type and accessibility of the given datastore
func getDatastoreProperties(ctx context.Context, d *cnsvsphere.DatastoreInfo) (
	*resource.Quantity, *resource.Quantity, string, string, bool, bool) {
	log := logger.GetLogger(ctx)
	var ds mo.Datastore
	pc := property.DefaultCollector(d.Client())
	err := pc.RetrieveOne(ctx, d.Reference(), []string{"summary"}, &ds)
	if err != nil {
		log.Errorf("Error retrieving datastore summary for %v. Err: %v", d, err)
		return nil, nil, "", "", false, false
	}
	capacity := resource.NewQuantity(ds.Summary.Capacity, resource.DecimalSI)
	freeSpace := resource.NewQuantity(ds.Summary.FreeSpace, resource.DecimalSI)
	accessible := ds.Summary.Accessible
	inMM := ds.Summary.MaintenanceMode != string(vimtypes.DatastoreSummaryMaintenanceModeStateNormal)
	dsType := ds.Summary.Type

	log.Infof("Datastore %s properties: type %v, capacity %v, freeSpace %v, accessibility %t, "+
		"inMaintenanceMode %t", d.Info.Name, dsType, capacity, freeSpace, accessible, inMM)
	return capacity, freeSpace, ds.Summary.Url, spTypePrefix + dsType, accessible, inMM
}

// findAccessibleNodes returns the k8s node names of ESX hosts (limited to clusterID) on which
// the given datastore is mounted and accessible. The values in the map tells whether the ESX
// host is in Maintenance Mode.
func findAccessibleNodes(ctx context.Context, datastore *object.Datastore,
	clusterID string, vcclient *vim25.Client) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	clusterMoref := vimtypes.ManagedObjectReference{
		Type:  "ClusterComputeResource",
		Value: clusterID,
	}
	cluster := object.NewComputeResource(vcclient, clusterMoref)
	hosts, err := datastore.AttachedClusterHosts(ctx, cluster)
	if err != nil {
		log.Infof("Failed to get attached hosts of datastore %s, err=%+v", datastore.Reference().Value, err)
		return nil, err
	}

	// Now find the k8s node names of these hosts
	clientSet, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to create k8s client for cluster, err=%+v", err)
		return nil, err
	}
	nodeList, err := clientSet.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed getting all k8s nodes in cluster, err=%+v", err)
		return nil, err
	}

	hostMoIDTok8sName := make(map[string]string)
	for _, node := range nodeList.Items {
		hostMoid := node.ObjectMeta.Annotations["vmware-system-esxi-node-moid"]
		hostMoIDTok8sName[hostMoid] = node.ObjectMeta.Name
	}
	nodes := make(map[string]bool)
	for _, host := range hosts {
		thisName, ok := hostMoIDTok8sName[host.Reference().Value]
		if !ok || thisName == "" {
			// This host on which the datastore is mounted does not have the annotation yet.
			// So ignore this node and wait for next reconcile to pick this up.
			err = fmt.Errorf("waiting for node %s to get vmware-system-esxi-node-moid annotation", host.Reference().Value)
			continue
		}
		inMM, err := getHostInMaintenanceMode(ctx, host)
		if err != nil {
			log.Errorf("Error finding the host %s Maintenance Mode state: %v", host.Reference().Value, err)
			inMM = true
		}
		nodes[thisName] = inMM
	}
	log.Infof("Accessible nodes in MM for datastore %s: %v", datastore.Reference().Value, nodes)
	return nodes, err
}

// getHostInMaintenanceMode returns whether the given host is in MaintenanceMode
func getHostInMaintenanceMode(ctx context.Context, host *object.HostSystem) (bool, error) {
	// get host's runtime property
	var hs mo.HostSystem
	err := host.Properties(ctx, host.Reference(), []string{"runtime"}, &hs)
	if err != nil {
		return true, err
	}
	return hs.Runtime.InMaintenanceMode, nil
}

// makeStoragePoolName returns the given datastore name dsName with any non-alphanumeric chars replaced with '-'
func makeStoragePoolName(dsName string) string {
	reg, err := regexp.Compile(`[^\\.a-zA-Z0-9]+`)
	if err != nil {
		return dsName
	}
	spName := "storagepool-" + reg.ReplaceAllString(dsName, "-")
	// spName should be in lower case and should not end with "-"
	spName = strings.TrimSuffix(strings.ToLower(spName), "-")
	return spName
}

// getSPClient returns the StoragePool dynamic client
func getSPClient(ctx context.Context) (dynamic.Interface, *schema.GroupVersionResource, error) {
	log := logger.GetLogger(ctx)
	// Create a client to create/udpate StoragePool instances
	cfg, err := config.GetConfig()
	if err != nil {
		log.Errorf("Failed to get Kubernetes config. Err: %+v", err)
		return nil, nil, err
	}
	spclient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Errorf("Failed to create StoragePool client using config. Err: %+v", err)
		return nil, nil, err
	}
	spResource := spv1alpha1.SchemeGroupVersion.WithResource("storagepools")
	return spclient, &spResource, nil
}

// updateSPTypeInSC adds the datastore type as an annotation in the given StorageClass
func updateSPTypeInSC(ctx context.Context, scName, dsType string) error {
	log := logger.GetLogger(ctx)
	clientSet, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to create k8s client for cluster, err=%+v", err)
		return err
	}
	sc, err := clientSet.StorageV1().StorageClasses().Get(scName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Failed to get storage class object from cluster, err=%+v", err)
		return err
	}
	annotations := sc.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}
	compatSPTypes, ok := annotations[spTypeAnnotationKey]
	if !ok {
		// Create a new annotation by name
		annotations[spTypeAnnotationKey] = dsType
	} else {
		// Check if dsType is already present in the annotation value
		spTypes := strings.Split(compatSPTypes, ",")
		for _, spType := range spTypes {
			if spType == dsType {
				// dsType is already present
				return nil
			}
		}
		// Create a comma-separated list of dsTypes as an annotation value
		compatSPTypes += "," + dsType
		annotations[spTypeAnnotationKey] = compatSPTypes
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				spTypeAnnotationKey: annotations[spTypeAnnotationKey],
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Errorf("Failed to marshal patch(%s): %s", patch, err)
		return err
	}
	// Patch the storage class with the updated annotation
	updatedSC, err := clientSet.StorageV1().StorageClasses().Patch(scName, k8stypes.MergePatchType, patchBytes)
	if err != nil {
		log.Errorf("Failed to patch the storage class object with dsType. Err = %+v", err)
		return err
	}
	log.Debug("Successfully updated annotations of storage class: ", scName, updatedSC.Annotations)
	return nil
}

// Returns the StoragePolicyId referred by the given StorageClass. Needs a util function since vSphere CSI supports
// case insensitive parameters in the StorageClass.
func getStoragePolicyIDFromSC(sc *v1.StorageClass) string {
	if sc.Provisioner != types.Name {
		return ""
	}
	// vSphere CSI supports case insensitive parameters
	for key, value := range sc.Parameters {
		if strings.ToLower(key) == common.AttributeStoragePolicyID {
			return value
		}
	}
	return ""
}
