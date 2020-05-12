/*
Copyright 2020 VMware, Inc.

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
	"fmt"
	"regexp"
	"strings"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

// getDatastoreProperties returns the total capacity and accessebility of the given datastore
func getDatastoreProperties(ctx context.Context, d *cnsvsphere.DatastoreInfo) (*resource.Quantity, *resource.Quantity, error) {
	log := logger.GetLogger(ctx)
	var ds mo.Datastore
	pc := property.DefaultCollector(d.Client())
	err := pc.RetrieveOne(ctx, d.Reference(), []string{"summary"}, &ds)
	if err != nil {
		log.Errorf("Error retrieving datastore summary for %v. Err: %v", d, err)
		return nil, nil, err
	}
	capacity := resource.NewQuantity(ds.Summary.Capacity, resource.DecimalSI)
	freeSpace := resource.NewQuantity(ds.Summary.FreeSpace, resource.DecimalSI)
	accessible := ds.Summary.Accessible
	log.Infof("Setting capacity, freeSpace and accessebility of datastore %v to %v, %v and %v respectively", d.Info.Name, capacity, freeSpace, accessible)
	
	if !accessible {
		err = fmt.Errorf("Datastore not accessible") 
	}
	
	return capacity, freeSpace, err
}

// findAccessibleNodes returns the k8s node names of ESX hosts (limited to clusterID) on which
// the given datastore is mounted and accessible.
func findAccessibleNodes(ctx context.Context, datastore *cnsvsphere.DatastoreInfo,
	clusterID string, vcclient *vim25.Client) ([]string, error) {
	log := logger.GetLogger(ctx)
	clusterMoref := vimtypes.ManagedObjectReference{
		Type:  "ClusterComputeResource",
		Value: clusterID,
	}
	cluster := object.NewComputeResource(vcclient, clusterMoref)
	hosts, err := datastore.AttachedClusterHosts(ctx, cluster)
	if err != nil {
		log.Errorf("Failed to get attached hosts of datastore, err=%+v", err)
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
	nodeNames := make([]string, 0)
	for _, host := range hosts {
		nodeNames = append(nodeNames, hostMoIDTok8sName[host.Reference().Value])
	}
	log.Infof("Accessible nodes for datastore %s: %v", datastore.Info.Name, nodeNames)
	return nodeNames, nil
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