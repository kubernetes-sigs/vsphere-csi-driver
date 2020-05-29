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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

const spTypePrefix = "cns.vmware.com/"

// getDatastoreProperties returns the total capacity, freeSpace, URL, type and accessibility of the given datastore
func getDatastoreProperties(ctx context.Context, d *cnsvsphere.DatastoreInfo) (*resource.Quantity, *resource.Quantity, string, string, bool) {
	log := logger.GetLogger(ctx)
	var ds mo.Datastore
	pc := property.DefaultCollector(d.Client())
	err := pc.RetrieveOne(ctx, d.Reference(), []string{"summary"}, &ds)
	if err != nil {
		log.Errorf("Error retrieving datastore summary for %v. Err: %v", d, err)
		return nil, nil, "", "", false
	}
	capacity := resource.NewQuantity(ds.Summary.Capacity, resource.DecimalSI)
	freeSpace := resource.NewQuantity(ds.Summary.FreeSpace, resource.DecimalSI)
	accessible := ds.Summary.Accessible
	dsType := ds.Summary.Type

	log.Infof("Setting type, capacity, freeSpace and accessibility of datastore %v to %v, %v, %v and %v respectively",
		d.Info.Name, dsType, capacity, freeSpace, accessible)
	return capacity, freeSpace, ds.Summary.Url, spTypePrefix + dsType, accessible
}

// findAccessibleNodes returns the k8s node names of ESX hosts (limited to clusterID) on which
// the given datastore is mounted and accessible.
func findAccessibleNodes(ctx context.Context, datastore *object.Datastore,
	clusterID string, vcclient *vim25.Client) ([]string, error) {
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
	nodeNames := make([]string, 0)
	for _, host := range hosts {
		thisName, ok := hostMoIDTok8sName[host.Reference().Value]
		if !ok || thisName == "" {
			// This host on which the datastore is mounted does not have the annotation yet.
			// So ignore this node and wait for next reconcile to pick this up.
			err = fmt.Errorf("waiting for node %s to get vmware-system-esxi-node-moid annotation", host.Reference().Value)
			continue
		}
		nodeNames = append(nodeNames, thisName)
	}
	log.Infof("Accessible nodes for datastore %s: %v", datastore.Reference().Value, nodeNames)
	return nodeNames, err
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
