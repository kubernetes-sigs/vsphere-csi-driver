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
	"regexp"
	"strings"
	"time"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

const (
	spTypePrefix        = "cns.vmware.com/"
	spTypeLabelKey      = spTypePrefix + "StoragePoolType"
	spTypeAnnotationKey = spTypePrefix + "StoragePoolTypeHint"
	vsanDsType          = spTypePrefix + "vsan"
	nodeMoidAnnotation  = "vmware-system-esxi-node-moid"
)

type dsProps struct {
	dsName      string
	dsURL       string
	dsType      string
	containerID string
	inMM        bool
	accessible  bool
	capacity    *resource.Quantity
	freeSpace   *resource.Quantity
}

// getDatastoreProperties returns the total capacity, freeSpace, URL, type and
// accessibility of the given datastore.
func getDatastoreProperties(ctx context.Context, d *cnsvsphere.DatastoreInfo) *dsProps {
	log := logger.GetLogger(ctx)
	var ds mo.Datastore
	pc := property.DefaultCollector(d.Client())
	err := pc.RetrieveOne(ctx, d.Reference(), []string{"summary", "info"}, &ds)
	if err != nil {
		log.Errorf("Error retrieving datastore summary for %v. Err: %v", d, err)
		return nil
	}
	p := dsProps{
		dsName:      ds.Summary.Name,
		dsURL:       ds.Summary.Url,
		dsType:      spTypePrefix + ds.Summary.Type,
		containerID: ds.Info.GetDatastoreInfo().ContainerId,
		inMM:        ds.Summary.MaintenanceMode != string(vimtypes.DatastoreSummaryMaintenanceModeStateNormal),
		accessible:  ds.Summary.Accessible,
		capacity:    resource.NewQuantity(ds.Summary.Capacity, resource.DecimalSI),
		freeSpace:   resource.NewQuantity(ds.Summary.FreeSpace, resource.DecimalSI),
	}

	log.Infof("Datastore %s properties: %v", d.Info.Name, p)
	return &p
}

// getHostMoIDToK8sNameMap looks up the hostMoid annotation on each k8s node.
func getHostMoIDToK8sNameMap(ctx context.Context) (map[string]string, error) {
	log := logger.GetLogger(ctx)
	hostMoIDTok8sName := make(map[string]string)
	clientSet, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to create k8s client for cluster, err=%+v", err)
		return hostMoIDTok8sName, err
	}
	// TODO: Replace this direct API call with an informer.
	nodeList, err := clientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("Failed getting all k8s nodes in cluster, err=%+v", err)
		return hostMoIDTok8sName, err
	}

	for _, node := range nodeList.Items {
		hostMoid := node.ObjectMeta.Annotations[nodeMoidAnnotation]
		hostMoIDTok8sName[hostMoid] = node.ObjectMeta.Name
	}
	return hostMoIDTok8sName, nil
}

// findAccessibleNodes returns the k8s node names of ESX hosts (limited to
// clusterID) on which the given datastore is mounted and accessible. The
// values in the map tells whether the ESX host is in Maintenance Mode.
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

	// Now find the k8s node names of these hosts.
	hostMoIDTok8sName, err := getHostMoIDToK8sNameMap(ctx)
	nodes := make(map[string]bool)
	for _, host := range hosts {
		thisName, ok := hostMoIDTok8sName[host.Reference().Value]
		if !ok || thisName == "" {
			// This host on which the datastore is mounted does not have the
			// annotation yet. So ignore this node and wait for next reconcile
			// to pick this up.
			log.Debugf("Ignoring node %s without vmware-system-esxi-node-moid annotation", host.Reference().Value)
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

// getHostInMaintenanceMode returns whether the given host is in MaintenanceMode.
func getHostInMaintenanceMode(ctx context.Context, host *object.HostSystem) (bool, error) {
	// Get host's runtime property.
	var hs mo.HostSystem
	err := host.Properties(ctx, host.Reference(), []string{"runtime"}, &hs)
	if err != nil {
		return true, err
	}
	return hs.Runtime.InMaintenanceMode, nil
}

// makeStoragePoolName returns the given datastore name dsName with any
// non-alphanumeric chars replaced with '-'.
func makeStoragePoolName(dsName string) string {
	reg, err := regexp.Compile(`[^\\.a-zA-Z0-9]+`)
	if err != nil {
		return dsName
	}
	spName := "storagepool-" + reg.ReplaceAllString(dsName, "-")
	// spName should be in lower case and should not end with "-".
	spName = strings.TrimSuffix(strings.ToLower(spName), "-")
	return spName
}

// getSPClient returns the StoragePool dynamic client.
func getSPClient(ctx context.Context) (dynamic.Interface, *schema.GroupVersionResource, error) {
	log := logger.GetLogger(ctx)
	// Create a client to create/udpate StoragePool instances.
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

// updateSPTypeInSC adds the datastore type as an annotation in the given
// StorageClass.
func updateSPTypeInSC(ctx context.Context, scName, dsType string) error {
	log := logger.GetLogger(ctx)
	clientSet, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to create k8s client for cluster, err=%+v", err)
		return err
	}
	sc, err := clientSet.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
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
		// Create a new annotation by name.
		annotations[spTypeAnnotationKey] = dsType
	} else {
		// Check if dsType is already present in the annotation value.
		spTypes := strings.Split(compatSPTypes, ",")
		for _, spType := range spTypes {
			if spType == dsType {
				// dsType is already present.
				return nil
			}
		}
		// Create a comma-separated list of dsTypes as an annotation value.
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
	// Patch the storage class with the updated annotation.
	updatedSC, err := clientSet.StorageV1().StorageClasses().Patch(ctx,
		scName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		log.Errorf("Failed to patch the storage class object with dsType. Err = %+v", err)
		return err
	}
	log.Debug("Successfully updated annotations of storage class: ", scName, updatedSC.Annotations)
	return nil
}

// Returns the StoragePolicyId referred by the given StorageClass. Needs a util
// function since vSphere CSI supports case insensitive parameters in the
// StorageClass.
func getStoragePolicyIDFromSC(sc *v1.StorageClass) string {
	if sc.Provisioner != types.Name {
		return ""
	}
	// vSphere CSI supports case insensitive parameters.
	for key, value := range sc.Parameters {
		if strings.ToLower(key) == common.AttributeStoragePolicyID {
			return value
		}
	}
	return ""
}

// updateDrainStatus updates the status of the disk decommission request.
// NewStatus can either be "done" or "fail". In case of "fail" we also add the
// reason for failure given by errorString. In case of "done" errorString is
// not considered.
func updateDrainStatus(ctx context.Context, storagePoolName string, newStatus string, errorString string) error {
	log := logger.GetLogger(ctx)
	task := func() (done bool, err error) {
		k8sDynamicClient, spResource, err := getSPClient(ctx)
		if err != nil {
			return false, err
		}
		// Try to get the current drain Mode.
		sp, err := k8sDynamicClient.Resource(*spResource).Get(ctx, storagePoolName, metav1.GetOptions{})
		if err != nil {
			log.Errorf("Could not get StoragePool with name %v. Error: %v", storagePoolName, err)
			return false, err
		}
		drainMode, _, _ := unstructured.NestedString(sp.Object, "spec", "parameters", drainModeField)

		if drainMode == fullDataEvacuationMM || drainMode == ensureAccessibilityMM || drainMode == noMigrationMM {
			var patch map[string]interface{}
			if newStatus == drainFailStatus {
				log.Infof("Errorstring: %v", errorString)
				patch = map[string]interface{}{
					"status": map[string]interface{}{
						"diskDecomm": map[string]interface{}{
							drainStatusField:     newStatus,
							drainFailReasonField: errorString,
						},
					},
				}
			} else {
				patch = map[string]interface{}{
					"status": map[string]interface{}{
						"diskDecomm": map[string]interface{}{
							drainStatusField: newStatus,
						},
					},
				}
			}
			patchBytes, err := json.Marshal(patch)
			if err != nil {
				log.Errorf("Could not marshal patch for drain label. Error: %v", err)
				return false, err
			}

			updatedSP, err := k8sDynamicClient.Resource(*spResource).Patch(ctx,
				storagePoolName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				log.Errorf("Failed to update StoragePool instance %v with new drain status %v. Error %v",
					newStatus, storagePoolName, err)
				return false, err
			}
			log.Debugf("Successfully updated drain status to %v in StoragePool %v", newStatus, updatedSP.GetName())
			return true, nil
		}
		// If the decommMode was not found or current mode is not
		// "evacuateAll"/"ensureAccessibility" then simply return.
		return true, nil
	}

	baseDuration := time.Duration(100) * time.Millisecond
	thresholdDuration := time.Duration(10) * time.Second
	_, err := ExponentialBackoff(task, baseDuration, thresholdDuration, 1.5, 10)
	return err
}

// getDrainMode gets the disk decommission mode for a given StoragePool.
func getDrainMode(ctx context.Context, storagePoolName string) (mode string, found bool, err error) {
	k8sDynamicClient, spResource, err := getSPClient(ctx)
	if err != nil {
		return "", false, err
	}
	sp, err := k8sDynamicClient.Resource(*spResource).Get(ctx, storagePoolName, metav1.GetOptions{})
	if err != nil {
		return "", false, err
	}
	mode, found, err = unstructured.NestedString(sp.Object, "spec", "parameters", drainModeField)
	return mode, found, err
}

func addTargetSPAnnotationOnPVC(ctx context.Context, pvcName, namespace,
	targetSPName string) (*unstructured.Unstructured, error) {
	log := logger.GetLogger(ctx)
	pvcResource := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				targetSPAnnotationKey: targetSPName,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Errorf("Failed to marshal json to add target SP annotation. Error: %v", err)
		return nil, err
	}

	var updatedPVC *unstructured.Unstructured
	task := func() (done bool, err error) {
		k8sDynamicClient, _, err := getSPClient(ctx)
		if err != nil {
			return false, err
		}

		updatedPVC, err = k8sDynamicClient.Resource(pvcResource).Namespace(namespace).Patch(ctx,
			pvcName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			log.Errorf("Failed to update target StoragePool annotation on PVC %v in ns %v. Error: %v",
				pvcName, namespace, err)
			return false, err
		}
		log.Debugf("Successfully updated target StoragePool information to %v. Updated PVC: %v",
			targetSPName, updatedPVC.GetName())
		return true, nil
	}
	baseDuration := time.Duration(100) * time.Millisecond
	thresholdDuration := time.Duration(10) * time.Second
	_, err = ExponentialBackoff(task, baseDuration, thresholdDuration, 1.5, 5)
	return updatedPVC, err
}

// ExponentialBackoff is an algorithm which is used to spread out repeated
// execution of task (usually reconnection or packet sending task to avoid
// network congestion or to decrease server load). In exponential backoff,
// wait time is increased exponentially till maxBackoffDuration. We have
// introduced jitter here to avoid thundering herd problem in future.
func ExponentialBackoff(task func() (bool, error), baseDuration, maxBackoffDuration time.Duration,
	multiplier float64, retries int) (done bool, err error) {
	jitter := 0.3
	backoffResetDuration := maxBackoffDuration * 3
	expBackoffManager := wait.NewExponentialBackoffManager(baseDuration, maxBackoffDuration,
		backoffResetDuration, multiplier, jitter, clock.RealClock{})

	var timer clock.Timer
	for i := 0; i < retries; i++ {
		timer = expBackoffManager.Backoff()
		done, err = func() (bool, error) {
			defer runtime.HandleCrash()
			done, err := task()
			return done, err
		}()
		if done {
			return done, err
		}

		<-timer.C()
	}
	return done, err
}

// RetryOnError retries the given task function in case of error till it
// succeeds. Each retried is exponentially backed off as per the parameter
// provided to the function. Wait time starts from baseDuration and on each
// error wait time is exponentially increased by the provided multiplier till
// maxBackoffDuration. example input:
// RetryOnError(func() error { return vc.ConnectVsan(ctx) },
//    time.Duration(100) * time.Millisecond, time.Duration(10) * time.Second,
//    1.5)
func RetryOnError(task func() error, baseDuration, maxBackoffDuration time.Duration,
	multiplier float64, maxRetries int) error {
	taskFunc := func() (done bool, _ error) {
		err := task()
		if err != nil {
			return false, err
		}
		return true, nil
	}
	_, err := ExponentialBackoff(taskFunc, baseDuration, maxBackoffDuration, multiplier, maxRetries)
	return err
}
