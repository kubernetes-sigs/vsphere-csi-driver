/*
Copyright 2019 The Kubernetes Authors.

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

package wcpguest

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	vmoperatorv1alpha1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmoperatorv1alpha2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoperatorv1alpha3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

var (
	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
	}
)

type controller struct {
	supervisorClient            clientset.Interface
	guestClient                 clientset.Interface
	supervisorSnapshotterClient snapshotterClientSet.Interface
	restClientConfig            *rest.Config
	vmOperatorClient            client.Client
	cnsOperatorClient           client.Client
	vmWatcher                   *cache.ListWatch
	supervisorNamespace         string
	tanzukubernetesClusterUID   string
	tanzukubernetesClusterName  string
	guestClusterDist            string
	csi.UnimplementedControllerServer
}

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *commonconfig.Config, version string) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("Initializing WCPGC CSI controller")
	var err error
	// connect to the CSI controller in supervisor cluster
	c.supervisorNamespace, err = commonconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		return err
	}
	c.tanzukubernetesClusterUID = config.GC.TanzuKubernetesClusterUID
	c.tanzukubernetesClusterName = config.GC.TanzuKubernetesClusterName
	c.guestClusterDist = config.GC.ClusterDistribution
	c.restClientConfig = k8s.GetRestClientConfigForSupervisor(ctx, config.GC.Endpoint, config.GC.Port)
	c.supervisorClient, err = k8s.NewSupervisorClient(ctx, c.restClientConfig)
	if err != nil {
		log.Errorf("failed to create supervisorClient. Error: %+v", err)
		return err
	}
	c.guestClient, err = k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("failed to create guestClient. Error: %+v", err)
		return err
	}
	c.supervisorSnapshotterClient, err = k8s.NewSupervisorSnapshotClient(ctx, c.restClientConfig)
	if err != nil {
		log.Errorf("failed to create supervisorSnapshotterClient. Error: %+v", err)
		return err
	}

	c.vmOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, vmoperatorv1alpha4.GroupName)
	if err != nil {
		log.Errorf("failed to create vmOperatorClient. Error: %+v", err)
		return err
	}

	c.cnsOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("failed to create cnsOperatorClient. Error: %+v", err)
		return err
	}
	c.vmWatcher, err = k8s.NewVirtualMachineWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
	if err != nil {
		log.Errorf("failed to create vmWatcher. Error: %+v", err)
		return err
	}

	// If workload-domain-isolation FSS is not enabled on guest cluster, then check the capabilities CR in
	// supervisor cluster every 2 mins to check if there is a change in Workload_Domain_Isolation_Supported
	// capability value from false to true. If so, restart the CSI controller container on guest.
	// NOTE: We can add other capabilities here when similar functionality is required. For
	// workload-isolation-domain feature we are restarting the container when capability changes dynamically from
	// false to true, but for other features instead of restarting CSI container, if possible we can implement
	// some init() function which can initialize required things when capability value changes from false to true.
	isWorkloadDomainIsolationSupported := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.WorkloadDomainIsolationFSS)
	linkedClonePVCSIFSS := commonco.ContainerOrchestratorUtility.IsPVCSIFSSEnabled(ctx, common.LinkedCloneSupportFSS)
	linkedCloneCapability := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.LinkedCloneSupportFSS)
	if !isWorkloadDomainIsolationSupported {
		go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx, cnstypes.CnsClusterFlavorGuest,
			common.WorkloadDomainIsolation, config.GC.Port, config.GC.Endpoint)
	}
	// Start the late enablement watcher only if the PVCSI internal FSS is enabled, but the current supervisor
	// capability is disabled.
	if linkedClonePVCSIFSS && !linkedCloneCapability {
		go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx, cnstypes.CnsClusterFlavorGuest,
			common.LinkedCloneSupport, config.GC.Port, config.GC.Endpoint)
	}
	if isWorkloadDomainIsolationSupported {
		err := commonco.ContainerOrchestratorUtility.StartZonesInformer(ctx, c.restClientConfig, c.supervisorNamespace)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to listen on zones CR. Error: %v", err)
		}
	}

	pvcsiConfigPath := commonconfig.GetConfigPath(ctx)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create fsnotify watcher. err=%v", err)
		return err
	}

	go func() {
		for {
			log.Debugf("Waiting for event on fsnotify watcher")
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Debugf("fsnotify event: %q", event.String())
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					for {
						reloadConfigErr := c.ReloadConfiguration()
						if reloadConfigErr == nil {
							log.Infof("Successfully reloaded configuration from: %q", pvcsiConfigPath)
							break
						}
						log.Errorf("failed to reload configuration. will retry again in 5 seconds. err: %+v", reloadConfigErr)
						time.Sleep(5 * time.Second)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Errorf("fsnotify error: %+v", err)
			}
			log.Debugf("fsnotify event processed")
		}
	}()
	cfgDirPath := filepath.Dir(pvcsiConfigPath)
	log.Infof("Adding watch on path: %q", cfgDirPath)
	err = watcher.Add(cfgDirPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", cfgDirPath, err)
		return err
	}
	log.Infof("Adding watch on path: %q", commonconfig.DefaultpvCSIProviderPath)
	err = watcher.Add(commonconfig.DefaultpvCSIProviderPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", commonconfig.DefaultpvCSIProviderPath, err)
		return err
	}
	// Go module to keep the metrics http server running all the time.
	go func() {
		prometheus.CsiInfo.WithLabelValues(version).Set(1)
		for {
			log.Info("Starting the http server to expose Prometheus metrics..")
			http.Handle("/metrics", promhttp.Handler())
			err = http.ListenAndServe(":2112", nil)
			if err != nil {
				log.Warnf("Http server that exposes the Prometheus exited with err: %+v", err)
			}
			log.Info("Restarting http server to expose Prometheus metrics..")
		}
	}()
	return nil
}

// ReloadConfiguration reloads configuration from the secret, and reset restClientConfig, supervisorClient
// and re-create vmOperatorClient using new config
func (c *controller) ReloadConfiguration() error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Info("Reloading Configuration")
	cfg, err := commonconfig.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return err
	}
	if cfg != nil {
		c.restClientConfig = k8s.GetRestClientConfigForSupervisor(ctx, cfg.GC.Endpoint, cfg.GC.Port)
		c.supervisorClient, err = k8s.NewSupervisorClient(ctx, c.restClientConfig)
		if err != nil {
			log.Errorf("failed to create supervisorClient. Error: %+v", err)
			return err
		}
		c.guestClient, err = k8s.NewClient(ctx)
		if err != nil {
			log.Errorf("failed to create guestClient. Error: %+v", err)
			return err
		}
		c.supervisorSnapshotterClient, err = k8s.NewSupervisorSnapshotClient(ctx, c.restClientConfig)
		if err != nil {
			log.Errorf("failed to create supervisorSnapshotterClient. Error: %+v", err)
			return err
		}
		log.Infof("successfully re-created supervisorClient using updated configuration")
		c.vmOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, vmoperatorv1alpha4.GroupName)
		if err != nil {
			log.Errorf("failed to create vmOperatorClient. Error: %+v", err)
			return err
		}
		c.vmWatcher, err = k8s.NewVirtualMachineWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
		if err != nil {
			log.Errorf("failed to create vmWatcher. Error: %+v", err)
			return err
		}
		log.Infof("successfully re-created vmOperatorClient using updated configuration")
		c.cnsOperatorClient, err = k8s.NewClientForGroup(ctx, c.restClientConfig, cnsoperatorv1alpha1.GroupName)
		if err != nil {
			log.Errorf("failed to create cnsOperatorClient. Error: %+v", err)
			return err
		}
	}
	return nil
}

// CreateVolume is creating CNS Volume using volume request specified
// in CreateVolumeRequest
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType
	createVolumeInternal := func() (
		*csi.CreateVolumeResponse, string, error) {

		var (
			pvcName              string
			pvcNamespace         string
			isLinkedCloneRequest bool
		)
		log.Infof("CreateVolume: called with args %+v", req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		err := validateGuestClusterCreateVolumeRequest(ctx, req)
		if err != nil {
			log.Errorf("validation for CreateVolume Request: %+v has failed. Error: %+v",
				req, err)
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		isFileVolumeRequest := common.IsFileVolumeRequest(ctx, req.GetVolumeCapabilities())
		if isFileVolumeRequest {
			volumeType = prometheus.PrometheusFileVolumeType
		} else {
			volumeType = prometheus.PrometheusBlockVolumeType
		}

		// Get PVC name and disk size for the supervisor cluster
		// We use default prefix 'pvc-' for pvc created in the guest cluster, it is mandatory.
		supervisorPVCName := c.tanzukubernetesClusterUID + "-" + req.Name[4:]
		var volumeSnapshotName string

		// Volume Size - Default is 10 GiB
		volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
		if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
			volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
		}
		volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))
		volumeSource := req.GetVolumeContentSource()
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot) &&
			volumeSource != nil {
			sourceSnapshot := volumeSource.GetSnapshot()
			if sourceSnapshot == nil {
				return nil, csifault.CSIInvalidArgumentFault,
					logger.LogNewErrorCode(log, codes.InvalidArgument, "unsupported VolumeContentSource type")
			}
			volumeSnapshotName = sourceSnapshot.GetSnapshotId()
		}

		// Get supervisorStorageClass and accessMode
		var supervisorStorageClass string
		for param := range req.Parameters {
			paramName := strings.ToLower(param)
			if paramName == common.AttributeSupervisorStorageClass {
				supervisorStorageClass = req.Parameters[param]
			}
			if paramName == common.AttributePvcName {
				pvcName = req.Parameters[param]
			}
			if paramName == common.AttributePvcNamespace {
				pvcNamespace = req.Parameters[param]
			}
		}

		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.LinkedCloneSupportFSS) {
			// Check if this is a LinkedClone request
			isLinkedCloneRequest, err = commonco.ContainerOrchestratorUtility.IsLinkedCloneRequest(ctx, pvcName, pvcNamespace)
			if err != nil {
				msg := fmt.Sprintf("failed to check if pvc with name: %s on namespace: %s in guest is a linked clone "+
					"request. Error: %+v", pvcName, pvcNamespace, err)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}

			if isFileVolumeRequest && isLinkedCloneRequest {
				msg := "cannot create a linked clone volume for a file volume"
				return nil, csifault.CSIInternalFault, status.Error(codes.FailedPrecondition, msg)
			}

			if isLinkedCloneRequest {
				// fallback to the mutating webhook to add the linked clone label
				err = commonco.ContainerOrchestratorUtility.PreLinkedCloneCreateAction(ctx, pvcName, pvcNamespace)
				if err != nil {
					msg := fmt.Sprintf("failed to add linked clone label on pvc %s/%s in guest. Error: %+v",
						pvcNamespace, pvcName, err)
					return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
				}
			}
		}
		accessMode := req.GetVolumeCapabilities()[0].GetAccessMode().GetMode()
		pvc, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, supervisorPVCName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				diskSize := strconv.FormatInt(volSizeMB, 10) + "Mi"
				labels := make(map[string]string)
				annotations := make(map[string]string)
				key := fmt.Sprintf("%s/%s", c.tanzukubernetesClusterName, c.guestClusterDist)
				labels[key] = c.tanzukubernetesClusterUID
				if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) &&
					req.AccessibilityRequirements != nil &&
					!isLinkedCloneRequest && // the cns-csi mutation webhook in supervisor will automatically set it.
					(commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WorkloadDomainIsolationFSS) ||
						!isFileVolumeRequest) {
					// Generate volume topology requirement annotation.
					topologyAnnotation, err := generateGuestClusterRequestedTopologyJSON(req.AccessibilityRequirements.Preferred)
					if err != nil {
						msg := fmt.Sprintf("failed to generate accessibility topology for pvc with name: %s "+
							"on namespace: %s from supervisorCluster. Error: %+v",
							supervisorPVCName, c.supervisorNamespace, err)
						return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
					}
					annotations[common.AnnGuestClusterRequestedTopology] = topologyAnnotation
				}
				// Add CnsVolumeFinalizer to Supervisor PVC if SVPVCSnapshotProtectionFinalizer FSS is enabled
				finalizers := []string{}
				if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
					common.SVPVCSnapshotProtectionFinalizer) {
					finalizers = append(finalizers, cnsoperatortypes.CNSVolumeFinalizer)
				}

				if isLinkedCloneRequest && commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
					common.LinkedCloneSupportFSS) {
					labels[common.LinkedClonePVCLabel] = "true"
				}
				claim := getPersistentVolumeClaimSpecWithStorageClass(supervisorPVCName, c.supervisorNamespace,
					diskSize, supervisorStorageClass, getAccessMode(accessMode), annotations, labels, finalizers,
					volumeSnapshotName, isLinkedCloneRequest)
				log.Debugf("PVC claim spec is %+v", spew.Sdump(claim))
				pvc, err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Create(
					ctx, claim, metav1.CreateOptions{})
				if err != nil {
					msg := fmt.Sprintf("failed to create pvc with name: %s on namespace: %s in supervisorCluster. Error: %+v",
						supervisorPVCName, c.supervisorNamespace, err)
					log.Error(msg)
					return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
				}
			} else {
				msg := fmt.Sprintf("failed to get pvc with name: %s on namespace: %s from supervisorCluster. Error: %+v",
					supervisorPVCName, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}
		}
		isBound, err := isPVCInSupervisorClusterBound(ctx, c.supervisorClient,
			pvc, time.Duration(getProvisionTimeoutInMin(ctx))*time.Minute)
		if !isBound {
			msg := fmt.Sprintf("failed to create volume on namespace: %s in supervisor cluster. Error: %+v",
				c.supervisorNamespace, err)
			log.Error(msg)
			eventList, err := c.supervisorClient.CoreV1().Events(c.supervisorNamespace).List(ctx,
				metav1.ListOptions{
					FieldSelector:        "involvedObject.name=" + pvc.Name,
					ResourceVersion:      pvc.ResourceVersion,
					ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
				})
			if err != nil {
				log.Errorf("Unable to fetch events for pvc %q/%q from supervisor cluster with err: %+v",
					c.supervisorNamespace, pvc.Name, err)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}

			var failureMessage string
			for _, svcPvcEvent := range eventList.Items {
				if svcPvcEvent.Type == corev1.EventTypeWarning {
					failureMessage = svcPvcEvent.Message
					break
				}
			}

			if failureMessage != "" {
				msg = fmt.Sprintf("%s. reason: %s", msg, failureMessage)
			}

			log.Errorf("Last observed events on the pvc %q/%q in supervisor cluster: %+v",
				c.supervisorNamespace, pvc.Name, spew.Sdump(eventList.Items))
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		attributes := make(map[string]string)
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) && isFileVolumeRequest {
			attributes[common.AttributeDiskType] = common.DiskTypeFileVolume
		} else {
			attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
		}

		if isLinkedCloneRequest {
			// Add a linked clone attribute to PV to be able to determine the volume is a LinkedClone even if the PVC
			// is deleted.
			volumeSnapshotUID, err := commonco.ContainerOrchestratorUtility.
				GetLinkedCloneVolumeSnapshotSourceUUID(ctx, pvcName, pvcNamespace)
			if err != nil {
				msg := fmt.Sprintf("failed to get linked clone name: %s on namespace: %s source volumesnapshot. "+
					"Error: %+v", pvcName, pvcNamespace, err)
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal, msg)
			}
			attributes[common.VolumeContextAttributeLinkedCloneVolumeSnapshotSourceUID] = volumeSnapshotUID
		}

		resp := &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      supervisorPVCName,
				CapacityBytes: int64(volSizeMB * common.MbInBytes),
				VolumeContext: attributes,
			},
		}

		// Set the Snapshot VolumeContentSource in the CreateVolumeResponse
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot) &&
			volumeSnapshotName != "" {
			resp.Volume.ContentSource = &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{
						SnapshotId: volumeSnapshotName,
					},
				},
			}
		}

		// Calculate node affinity terms for topology aware provisioning.
		var accessibleTopologies []map[string]string
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) &&
			req.AccessibilityRequirements != nil &&
			(commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WorkloadDomainIsolationFSS) ||
				!isFileVolumeRequest) {
			// Retrieve the latest version of the PVC
			pvc, err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
				ctx, supervisorPVCName, metav1.GetOptions{})
			if err != nil {
				msg := fmt.Sprintf("failed to get pvc with name: %s on namespace: %s from supervisorCluster. "+
					"Error: %+v", supervisorPVCName, c.supervisorNamespace, err)
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal, msg)
			}
			// Generate accessible topologies for volume.
			accessibleTopologies, err = generateVolumeAccessibleTopologyFromPVCAnnotation(pvc)
			if err != nil {
				msg := fmt.Sprintf("failed to generate volume accessible topology for pvc with name: %s on "+
					"namespace: %s from supervisorCluster requirements with err: %+v",
					c.supervisorNamespace, pvc.Name, err)
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCode(log, codes.Internal, msg)
			}
			log.Infof("Volume %q created is accessible from zones: %+v", supervisorPVCName,
				accessibleTopologies)

			// Add topology segments to the CreateVolumeResponse.
			for _, topoSegments := range accessibleTopologies {
				volumeTopology := &csi.Topology{
					Segments: topoSegments,
				}
				resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
			}
		}
		return resp, "", nil
	}
	resp, faultType, err := createVolumeInternal()
	log.Debugf("createVolumeInternal: returns fault %q", faultType)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusCreateVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume created successfully. Volume Handle: %q, PV Name: %q", resp.Volume.VolumeId, req.Name)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	log.Debugf("CreateVolume response: %+v", resp)
	return resp, err
}

// DeleteVolume is deleting CNS Volume specified in DeleteVolumeRequest
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {

	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType

	deleteVolumeInternal := func() (
		*csi.DeleteVolumeResponse, string, error) {
		log.Infof("DeleteVolume: called with args: %+v", req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.
		var err error
		err = validateGuestClusterDeleteVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for Delete Volume Request: %+v has failed. Error: %+v",
				req, err)
			log.Error(msg)
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		// Retrieve Supervisor PVC
		svPVC, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, req.VolumeId, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				log.Debugf("PVC: %q not found in the Supervisor cluster. Assuming the volume is already deleted.",
					req.VolumeId)
				return &csi.DeleteVolumeResponse{}, "", nil
			}
			msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v",
				req.VolumeId, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		for _, accessMode := range svPVC.Spec.AccessModes {
			if accessMode == corev1.ReadWriteMany || accessMode == corev1.ReadOnlyMany {
				volumeType = prometheus.PrometheusFileVolumeType
			}
		}
		// Remove the finalizer before deleting the Supervisor PVC if SVPVCSnapshotProtectionFinalizer FSS is enabled
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SVPVCSnapshotProtectionFinalizer) {
			for i, finalizer := range svPVC.ObjectMeta.Finalizers {
				if finalizer == cnsoperatortypes.CNSVolumeFinalizer {
					log.Infof("Removing %q finalizer from PersistentVolumeClaim with name: %q on namespace: %q",
						cnsoperatortypes.CNSVolumeFinalizer, svPVC.Name, svPVC.Namespace)
					svPVC.ObjectMeta.Finalizers = slices.Delete(svPVC.ObjectMeta.Finalizers, i, i+1)
					// Update the instance after removing finalizer
					_, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Update(ctx, svPVC,
						metav1.UpdateOptions{})
					if err != nil {
						msg := fmt.Sprintf("failed to update supervisor PVC %q in %q namespace. Error: %+v",
							req.VolumeId, c.supervisorNamespace, err)
						log.Error(msg)
						return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
					}
					break
				}
			}
		}

		// Delete Supervisor PVC
		err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Delete(
			ctx, req.VolumeId, *metav1.NewDeleteOptions(0))
		if err != nil {
			if errors.IsNotFound(err) {
				log.Debugf("PVC: %q not found in the Supervisor cluster. Assuming this volume to be deleted.",
					req.VolumeId)
				return &csi.DeleteVolumeResponse{}, "", nil
			}
			msg := fmt.Sprintf("DeleteVolume Request: %+v has failed. Error: %+v", req, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}

		// Wait for PVC to be deleted from supervisor cluster
		err = common.WaitForPVCDeleted(ctx, c.supervisorClient,
			req.VolumeId, c.supervisorNamespace,
			time.Duration(getProvisionTimeoutInMin(ctx))*time.Minute)
		if err != nil {
			msg := fmt.Sprintf("persistentVolumeClaim: %s on namespace: %s in supervisor cluster was not deleted. "+
				"Error: %+v", req.VolumeId, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		log.Infof("DeleteVolume: Volume deleted successfully. VolumeID: %q", req.VolumeId)
		return &csi.DeleteVolumeResponse{}, "", nil
	}
	resp, faultType, err := deleteVolumeInternal()
	log.Debugf("deleteVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusDeleteVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q deleted successfully.", req.VolumeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ControllerPublishVolume attaches a volume to the Node VM.
// volume id and node name is retrieved from ControllerPublishVolumeRequest
func (c *controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerPublishVolumeInternal := func() (
		*csi.ControllerPublishVolumeResponse, string, error) {
		log.Infof("ControllerPublishVolume: called with args %+v", req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.

		// Check whether the request is for a block or file volume
		isFileVolumeRequest := common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{req.GetVolumeCapability()})

		err := validateGuestClusterControllerPublishVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v",
				req, err)
			log.Error(msg)
			return nil, csifault.CSIInvalidArgumentFault, status.Error(codes.Internal, msg)
		}

		// File volumes support
		if isFileVolumeRequest {
			volumeType = prometheus.PrometheusFileVolumeType
			// Check the feature state for file volume support
			if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) {
				// Feature is disabled on the cluster
				return nil, csifault.CSIInternalFault,
					status.Error(codes.InvalidArgument, "File volume not supported.")
			}
			return controllerPublishForFileVolume(ctx, req, c)
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		// Block volumes support
		return controllerPublishForBlockVolume(ctx, req, c)
	}

	resp, faultType, err := controllerPublishVolumeInternal()
	if err != nil {
		log.Debugf("controllerPublishVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusAttachVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusAttachVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q attached successfully to node %q", req.VolumeId, req.NodeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusAttachVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// controllerPublishForBlockVolume is a helper mthod for handling ControllerPublishVolume request for Block volumes
func controllerPublishForBlockVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest, c *controller) (
	*csi.ControllerPublishVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	var isVolumePresentInSpec, isVolumeAttached bool
	var diskUUID string
	var err error

	virtualMachine := &vmoperatorv1alpha4.VirtualMachine{}
	vmKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      req.NodeId,
	}

	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	timeout := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	for {
		virtualMachine, _, err = utils.GetVirtualMachineAllApiVersions(
			ctx, vmKey, c.vmOperatorClient)
		if err != nil {
			msg := fmt.Sprintf("failed to get VirtualMachines for the node: %q. Error: %+v", req.NodeId, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		// Check if volume is already present in the virtualMachine.Spec.Volumes
		for _, volume := range virtualMachine.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.Name == req.VolumeId {
				log.Infof("Volume %q is already present in the virtualMachine.Spec.Volumes", volume.Name)
				isVolumePresentInSpec = true
				break
			}
		}
		if isVolumePresentInSpec {
			break
		}
		// Create a patch for the VM prior to modifying it with the new volumes.
		old_virtualMachine := virtualMachine.DeepCopy()
		// Volume is not present in the virtualMachine.Spec.Volumes, so adding
		// volume in the spec and patching virtualMachine instance.
		vmvolumes := vmoperatorv1alpha4.VirtualMachineVolume{
			Name: req.VolumeId,
			VirtualMachineVolumeSource: vmoperatorv1alpha4.VirtualMachineVolumeSource{
				PersistentVolumeClaim: &vmoperatorv1alpha4.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: req.VolumeId,
					},
				},
			},
		}
		virtualMachine.Spec.Volumes = append(virtualMachine.Spec.Volumes, vmvolumes)
		// Issue a patch with the modified VM against the patch created above.
		if err := utils.PatchVirtualMachine(ctx, c.vmOperatorClient, virtualMachine, old_virtualMachine); err == nil {
			break
		} else {
			log.Errorf("failed to update virtualmachine. Err: %v", err)
		}
		if time.Now().After(timeout) {
			msg := fmt.Sprintf("timedout to update VirtualMachines %q", virtualMachine.Name)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		virtualMachine = &vmoperatorv1alpha4.VirtualMachine{}
	}

	for _, volume := range virtualMachine.Status.Volumes {
		if volume.Name == req.VolumeId && volume.Attached && volume.DiskUUID != "" {
			diskUUID = volume.DiskUUID
			isVolumeAttached = true
			log.Infof("Volume %q is already attached in the virtualMachine.Spec.Volumes. Disk UUID: %q",
				volume.Name, volume.DiskUUID)
			break
		}
	}
	// volume is not attached, so wait until volume is attached and DiskUuid is set
	if !isVolumeAttached {
		watchVirtualMachine, err := c.vmWatcher.WatchWithContext(ctx, metav1.ListOptions{
			FieldSelector:  fields.SelectorFromSet(fields.Set{"metadata.name": string(virtualMachine.Name)}).String(),
			TimeoutSeconds: &timeoutSeconds,
		})
		if err != nil {
			msg := fmt.Sprintf("failed to watch virtualMachine %q with Error: %v", virtualMachine.Name, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		defer watchVirtualMachine.Stop()

		// Watch all update events made on VirtualMachine instance until volume.DiskUuid is set
		for diskUUID == "" {
			// blocking wait for update event
			log.Debugf("waiting for update on virtualmachine: %q", virtualMachine.Name)
			event := <-watchVirtualMachine.ResultChan()
			vm := &vmoperatorv1alpha4.VirtualMachine{}
			vm4, ok := event.Object.(*vmoperatorv1alpha4.VirtualMachine)
			if !ok {
				vm3, ok := event.Object.(*vmoperatorv1alpha3.VirtualMachine)
				if !ok {
					vm2, ok := event.Object.(*vmoperatorv1alpha2.VirtualMachine)
					if !ok {
						vm1, ok := event.Object.(*vmoperatorv1alpha1.VirtualMachine)
						if !ok {
							msg := fmt.Sprintf("Watch on virtualmachine %q timed out", virtualMachine.Name)
							log.Error(msg)
							return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
						} else {
							log.Infof("converting v1alpha1 VirtualMachine to v1alpha4 VirtualMachine, name %s", vm1.Name)
							err = vmoperatorv1alpha1.Convert_v1alpha1_VirtualMachine_To_v1alpha4_VirtualMachine(
								vm1, vm, nil)
							if err != nil {
								return nil, csifault.CSIInternalFault, status.Error(codes.Internal, err.Error())
							}
						}
					} else {
						log.Infof("converting v1alpha2 VirtualMachine to v1alpha4 VirtualMachine, name %s", vm2.Name)
						err = vmoperatorv1alpha2.Convert_v1alpha2_VirtualMachine_To_v1alpha4_VirtualMachine(
							vm2, vm, nil)
						if err != nil {
							return nil, csifault.CSIInternalFault, status.Error(codes.Internal, err.Error())
						}
					}
				} else {
					log.Infof("converting v1alpha2 VirtualMachine to v1alpha4 VirtualMachine, name %s", vm3.Name)
					err = vmoperatorv1alpha3.Convert_v1alpha3_VirtualMachine_To_v1alpha4_VirtualMachine(
						vm3, vm, nil)
					if err != nil {
						return nil, csifault.CSIInternalFault, status.Error(codes.Internal, err.Error())
					}
				}
			} else {
				vm = vm4
			}
			if vm.Name != virtualMachine.Name {
				log.Debugf("Observed vm name: %q, expecting vm name: %q, volumeID: %q",
					vm.Name, virtualMachine.Name, req.VolumeId)
				continue
			}
			log.Debugf("observed update on virtualmachine: %q. checking if disk UUID is set for volume: %q ",
				virtualMachine.Name, req.VolumeId)
			for _, volume := range vm.Status.Volumes {
				if volume.Name == req.VolumeId {
					if volume.Attached && volume.DiskUUID != "" && volume.Error == "" {
						diskUUID = volume.DiskUUID
						log.Infof("observed disk UUID %q is set for the volume %q on virtualmachine %q",
							volume.DiskUUID, volume.Name, vm.Name)
					} else {
						if volume.Error != "" {
							msg := fmt.Sprintf("observed Error: %q is set on the volume %q on virtualmachine %q",
								volume.Error, volume.Name, vm.Name)
							log.Error(msg)
							return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
						}
					}
					break
				}
			}
			if diskUUID == "" {
				log.Debugf("disk UUID is not set for volume: %q ", req.VolumeId)
			}
		}
		log.Debugf("disk UUID %v is set for the volume: %q ", diskUUID, req.VolumeId)
	}

	// return PublishContext with diskUUID of the volume attached to node.
	publishInfo := make(map[string]string)
	publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
	publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
	resp := &csi.ControllerPublishVolumeResponse{
		PublishContext: publishInfo,
	}
	log.Infof("ControllerPublishVolume: Volume attached successfully %q", req.VolumeId)
	return resp, "", nil
}

// controllerPublishForFileVolume is a helper mthod for handling ControllerPublishVolume request for File volumes
func controllerPublishForFileVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest, c *controller) (
	*csi.ControllerPublishVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	// Build the CnsFileAccessConfig instance name and namespace
	cnsFileAccessConfigInstance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	cnsFileAccessConfigInstanceName := req.NodeId + "-" + req.VolumeId
	cnsFileAccessConfigInstanceKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      cnsFileAccessConfigInstanceName,
	}

	// Check whether the CnsFileAccessConfig instance exist in the supervisor cluster
	if err := c.cnsOperatorClient.Get(ctx, cnsFileAccessConfigInstanceKey, cnsFileAccessConfigInstance); err != nil {
		if !errors.IsNotFound(err) {
			// Get() on the CnsFileAccessConfig instance failed with different error
			msg := fmt.Sprintf("failed to get CnsFileAccessConfig instance: %q/%q. Error: %+v",
				c.supervisorNamespace, cnsFileAccessConfigInstance.Name, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		// Create the CnsFileAccessConfig instance since it is not found
		cnsFileAccessConfigInstance = &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cnsFileAccessConfigInstanceName,
				Namespace: c.supervisorNamespace},
			Spec: cnsfileaccessconfigv1alpha1.CnsFileAccessConfigSpec{
				VMName:  req.NodeId,
				PvcName: req.VolumeId,
			},
		}
		log.Debugf("Creating CnsFileAccessConfig instance: %+v", cnsFileAccessConfigInstance)
		log.Infof("Creating CnsFileAccessConfig instance with name: %q", cnsFileAccessConfigInstance.Name)
		if err := c.cnsOperatorClient.Create(ctx, cnsFileAccessConfigInstance); err != nil {
			msg := fmt.Sprintf("failed to create cnsFileAccessConfig: %q/%q. Error: %v",
				c.supervisorNamespace, cnsFileAccessConfigInstance.Name, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
	}
	log.Debugf("Found CnsFileAccessConfig: %q/%q", c.supervisorNamespace, cnsFileAccessConfigInstance.Name)
	if cnsFileAccessConfigInstance.DeletionTimestamp != nil {
		// When deletionTimestamp is set, CnsOperator is in the process of
		// removing access for this IP. When that operation is successful, the
		// instance will be deleted. In a subsequent retry, a new instance will
		// be created.
		msg := fmt.Sprintf("cnsFileAccessConfigInstance %q/%q is getting deleted. "+
			"A new instance will be created in the subsequent ControllerPublishVolume request",
			c.supervisorNamespace, cnsFileAccessConfigInstance.Name)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}
	publishInfo := make(map[string]string)
	// Verify if the CnsFileAccessConfig instance has status with done set to true and error is empty
	if cnsFileAccessConfigInstance.Status.Done && cnsFileAccessConfigInstance.Status.Error == "" {
		for key, value := range cnsFileAccessConfigInstance.Status.AccessPoints {
			if key == common.Nfsv4AccessPointKey {
				publishInfo[common.Nfsv4AccessPoint] = value
				break
			}
		}
		publishInfo[common.AttributeDiskType] = common.DiskTypeFileVolume
		resp := &csi.ControllerPublishVolumeResponse{
			PublishContext: publishInfo,
		}
		log.Infof("ControllerPublishVolume: Volume %q attached successfully on the node: %q", req.VolumeId, req.NodeId)
		return resp, "", nil
	}
	cnsFileAccessConfigWatcher, err := k8s.NewCnsFileAccessConfigWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
	if err != nil {
		msg := fmt.Sprintf("failed to create cnsFileAccessConfigWatcher. Error: %+v", err)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}
	// Attacher timeout, default is set to 4 minutes
	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	// Adding watch on the CnsFileAccessConfig instance to register for updates
	watchCnsFileAccessConfig, err := cnsFileAccessConfigWatcher.WatchWithContext(ctx, metav1.ListOptions{
		FieldSelector:   fields.SelectorFromSet(fields.Set{"metadata.name": cnsFileAccessConfigInstance.Name}).String(),
		ResourceVersion: cnsFileAccessConfigInstance.ResourceVersion,
		TimeoutSeconds:  &timeoutSeconds,
	})
	if err != nil {
		msg := fmt.Sprintf("failed to watch cnsfileaccessconfig %q with Error: %v", cnsFileAccessConfigInstance.Name, err)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}
	defer watchCnsFileAccessConfig.Stop()
	var cnsFileAccessConfigInstanceErr string
	// Watch all update events made on CnsFileAccessConfig instance until accessPoints is set
	for {
		log.Debugf("Waiting for update on cnsfileaccessconfigs: %q", cnsFileAccessConfigInstance.Name)
		event := <-watchCnsFileAccessConfig.ResultChan()
		cnsfileaccessconfig, ok := event.Object.(*cnsfileaccessconfigv1alpha1.CnsFileAccessConfig)
		if !ok {
			msg := fmt.Sprintf("Watch on cnsfileaccessconfig instance %q timed out. Last seen error on the instance=%q",
				cnsFileAccessConfigInstance.Name, cnsFileAccessConfigInstanceErr)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		if cnsfileaccessconfig.Name != cnsFileAccessConfigInstanceName {
			log.Debugf("Observed cnsFileAccessConfig instance name: %q, expecting cnsFileAccessConfig instance name: %q",
				cnsfileaccessconfig.Name, cnsFileAccessConfigInstanceName)
			continue
		}
		// Check if SV PVC Name match with VolumeId from the request
		if cnsfileaccessconfig.Spec.PvcName != req.VolumeId {
			log.Debugf("Observed SV PVC Name: %q, expecting SV PVC Name: %q",
				cnsfileaccessconfig.Spec.PvcName, req.VolumeId)
			continue
		}
		// Check if VM name in the cnsfileaccessconfig instance match with NodeId from the request
		if cnsfileaccessconfig.Spec.VMName != req.NodeId {
			log.Debugf("Observed vm name: %q, expecting vm name: %q", cnsfileaccessconfig.Spec.VMName, req.NodeId)
			continue
		}
		log.Debugf("Observed an update on cnsfileaccessconfig: %+v", cnsfileaccessconfig)
		if cnsfileaccessconfig.Status.Done && cnsfileaccessconfig.Status.Error == "" &&
			cnsfileaccessconfig.DeletionTimestamp == nil {
			// Check if the updated instance has the AccessPoints
			for key, value := range cnsfileaccessconfig.Status.AccessPoints {
				if key == common.Nfsv4AccessPointKey {
					publishInfo[common.AttributeDiskType] = common.DiskTypeFileVolume
					publishInfo[common.Nfsv4AccessPoint] = value
					break
				}
			}
			if _, ok := publishInfo[common.Nfsv4AccessPoint]; ok {
				log.Debugf("Found Nfsv4AccessPoint in publishInfo. publishInfo=%+v", publishInfo)
				break
			}
		}
		cnsFileAccessConfigInstanceErr = cnsfileaccessconfig.Status.Error
	}
	resp := &csi.ControllerPublishVolumeResponse{
		PublishContext: publishInfo,
	}
	log.Infof("ControllerPublishVolume: Volume %q attached successfully on the node: %q", req.VolumeId, req.NodeId)
	return resp, "", nil
}

// ControllerUnpublishVolume detaches a volume from the Node VM.
// volume id and node name is retrieved from ControllerUnpublishVolumeRequest
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerUnpublishVolumeInternal := func() (
		*csi.ControllerUnpublishVolumeResponse, string, error) {
		log.Infof("ControllerUnpublishVolume: called with args %+v", req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.

		err := validateGuestClusterControllerUnpublishVolumeRequest(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v",
				req, err)
			log.Error(msg)
			return nil, csifault.CSIInvalidArgumentFault, err
		}

		// Retrieve Supervisor PVC
		svPVC, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, req.VolumeId, metav1.GetOptions{})
		if err != nil {
			msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v",
				req.VolumeId, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		var isFileVolume bool
		for _, accessMode := range svPVC.Spec.AccessModes {
			if accessMode == corev1.ReadWriteMany || accessMode == corev1.ReadOnlyMany {
				isFileVolume = true
			}
		}
		if isFileVolume {
			volumeType = prometheus.PrometheusFileVolumeType
			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) {
				return controllerUnpublishForFileVolume(ctx, req, c)
			}
			// Feature is disabled on the cluster
			return nil, csifault.CSIInvalidArgumentFault, status.Error(codes.InvalidArgument, "File volume not supported.")
		}
		volumeType = prometheus.PrometheusBlockVolumeType
		return controllerUnpublishForBlockVolume(ctx, req, c)
	}
	resp, faultType, err := controllerUnpublishVolumeInternal()
	log.Debugf("controllerUnpublishVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusDetachVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDetachVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q detached successfully from node %q.", req.VolumeId, req.NodeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDetachVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// controllerUnpublishForBlockVolume is helper method to handle ControllerPublishVolume for Block volumes
func controllerUnpublishForBlockVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest, c *controller) (
	*csi.ControllerUnpublishVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	virtualMachine := &vmoperatorv1alpha4.VirtualMachine{}
	vmKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      req.NodeId,
	}
	var err error
	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	timeout := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	for {
		virtualMachine, _, err = utils.GetVirtualMachineAllApiVersions(
			ctx, vmKey, c.vmOperatorClient)
		if err != nil {
			if errors.IsNotFound(err) {
				log.Infof("VirtualMachine %s/%s not found. Assuming volume %s was detached.",
					c.supervisorNamespace, req.NodeId, req.VolumeId)
				return &csi.ControllerUnpublishVolumeResponse{}, "", nil
			}
			msg := fmt.Sprintf("failed to get VirtualMachines for node: %q. Error: %+v", req.NodeId, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		log.Debugf("Found VirtualMachine for node: %q.", req.NodeId)

		for index, volume := range virtualMachine.Spec.Volumes {
			if volume.Name == req.VolumeId {
				log.Debugf("Removing volume %q from VirtualMachine %q", volume.Name, virtualMachine.Name)
				virtualMachine.Spec.Volumes = append(virtualMachine.Spec.Volumes[:index],
					virtualMachine.Spec.Volumes[index+1:]...)
				err = utils.UpdateVirtualMachine(ctx, c.vmOperatorClient, virtualMachine)
				break
			}
		}
		if err == nil {
			break
		} else {
			log.Errorf("failed to update virtualmachine. Err: %v", err)
		}
		if time.Now().After(timeout) {
			msg := fmt.Sprintf("timedout to update VirtualMachines %q", virtualMachine.Name)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		virtualMachine = &vmoperatorv1alpha4.VirtualMachine{}
	}
	isVolumePresentInVMStatus := false
	for _, volume := range virtualMachine.Status.Volumes {
		if volume.Name == req.VolumeId {
			isVolumePresentInVMStatus = true
		}
	}
	if !isVolumePresentInVMStatus {
		log.Infof("ControllerUnpublishVolume: Volume %q not found in VM %q status field. Assuming it's already detached",
			req.VolumeId, req.NodeId)
	} else {
		// Watch virtual machine object and wait for volume name to be removed from the status field.
		watchVirtualMachine, err := c.vmWatcher.WatchWithContext(ctx, metav1.ListOptions{
			FieldSelector:  fields.SelectorFromSet(fields.Set{"metadata.name": string(virtualMachine.Name)}).String(),
			TimeoutSeconds: &timeoutSeconds,
		})
		if err != nil {
			msg := fmt.Sprintf("failed to watch VirtualMachine %q with Error: %v", virtualMachine.Name, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		if watchVirtualMachine == nil {
			msg := fmt.Sprintf("watchVirtualMachine for %q is nil", virtualMachine.Name)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)

		}
		defer watchVirtualMachine.Stop()

		// Loop until the volume is removed from virtualmachine status
		isVolumeDetached := false
		for !isVolumeDetached {
			log.Debugf("Waiting for update on VirtualMachine: %q", virtualMachine.Name)
			// Block on update events
			event := <-watchVirtualMachine.ResultChan()
			vm := &vmoperatorv1alpha4.VirtualMachine{}
			vm4, ok := event.Object.(*vmoperatorv1alpha4.VirtualMachine)
			if !ok {
				vm3, ok := event.Object.(*vmoperatorv1alpha3.VirtualMachine)
				if !ok {
					vm2, ok := event.Object.(*vmoperatorv1alpha2.VirtualMachine)
					if !ok {
						vm1, ok := event.Object.(*vmoperatorv1alpha1.VirtualMachine)
						if !ok {
							msg := fmt.Sprintf("Watch on virtualmachine %q timed out", virtualMachine.Name)
							log.Error(msg)
							return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
						} else {
							err = vmoperatorv1alpha1.Convert_v1alpha1_VirtualMachine_To_v1alpha4_VirtualMachine(
								vm1, vm, nil)
							if err != nil {
								return nil, csifault.CSIInternalFault, status.Error(codes.Internal, err.Error())
							}
						}
					} else {
						err = vmoperatorv1alpha2.Convert_v1alpha2_VirtualMachine_To_v1alpha4_VirtualMachine(
							vm2, vm, nil)
						if err != nil {
							return nil, csifault.CSIInternalFault, status.Error(codes.Internal, err.Error())
						}
					}
				} else {
					err = vmoperatorv1alpha3.Convert_v1alpha3_VirtualMachine_To_v1alpha4_VirtualMachine(
						vm3, vm, nil)
					if err != nil {
						return nil, csifault.CSIInternalFault, status.Error(codes.Internal, err.Error())
					}
				}
			} else {
				vm = vm4
			}
			if vm.Name != virtualMachine.Name {
				log.Debugf("Observed vm name: %q, expecting vm name: %q, volumeID: %q",
					vm.Name, virtualMachine.Name, req.VolumeId)
				continue
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				isVolumeDetached = true
				for _, volume := range vm.Status.Volumes {
					if volume.Name == req.VolumeId {
						log.Debugf("Volume %q still exists in VirtualMachine %q status", volume.Name, virtualMachine.Name)
						isVolumeDetached = false
						if volume.Attached && volume.Error != "" {
							msg := fmt.Sprintf("failed to detach volume %q from VirtualMachine %q with Error: %v",
								volume.Name, virtualMachine.Name, volume.Error)
							log.Error(msg)
							return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
						}
						break
					}
				}
			case watch.Deleted:
				log.Infof("VirtualMachine %s/%s deleted. Assuming volume %s was detached.",
					c.supervisorNamespace, req.NodeId, req.VolumeId)
				isVolumeDetached = true
			}
		}
	}
	log.Infof("ControllerUnpublishVolume: Volume detached successfully %q", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, "", nil
}

// controllerUnpublishForFileVolume is helper method to handle ControllerPublishVolume for File volumes
func controllerUnpublishForFileVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest, c *controller) (
	*csi.ControllerUnpublishVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	// Adding watch on the CnsFileAccessConfig instance to register for updates
	cnsFileAccessConfigWatcher, err := k8s.NewCnsFileAccessConfigWatcher(ctx, c.restClientConfig, c.supervisorNamespace)
	if err != nil {
		msg := fmt.Sprintf("failed to create cnsFileAccessConfigWatcher. Error: %+v", err)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}
	cnsFileAccessConfigInstance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	cnsFileAccessConfigInstanceName := req.NodeId + "-" + req.VolumeId
	cnsFileAccessConfigInstanceKey := types.NamespacedName{
		Namespace: c.supervisorNamespace,
		Name:      cnsFileAccessConfigInstanceName,
	}
	if err := c.cnsOperatorClient.Get(ctx, cnsFileAccessConfigInstanceKey, cnsFileAccessConfigInstance); err != nil {
		if errors.IsNotFound(err) {
			log.Infof("ControllerUnpublishVolume: CnsFileAccessConfig instance %q/%q not found in supervisor cluster. "+
				"Returning success for the detach operation", c.supervisorNamespace, cnsFileAccessConfigInstanceName)
			return &csi.ControllerUnpublishVolumeResponse{}, "", nil
		}
		msg := fmt.Sprintf("failed to get CnsFileAccessConfig instance: %q/%q. Error: %+v",
			c.supervisorNamespace, cnsFileAccessConfigInstanceName, err)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}
	// Attach/Detach timeout, default is set to 4 minutes
	timeoutSeconds := int64(getAttacherTimeoutInMin(ctx) * 60)
	watchCnsFileAccessConfig, err := cnsFileAccessConfigWatcher.WatchWithContext(ctx, metav1.ListOptions{
		FieldSelector:   fields.SelectorFromSet(fields.Set{"metadata.name": cnsFileAccessConfigInstanceName}).String(),
		ResourceVersion: cnsFileAccessConfigInstance.ResourceVersion,
		TimeoutSeconds:  &timeoutSeconds,
	})
	if err != nil {
		msg := fmt.Sprintf("failed to watch cnsFileAccessConfig instance %q/%q with Error: %v",
			c.supervisorNamespace, cnsFileAccessConfigInstanceName, err)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}
	if err := c.cnsOperatorClient.Delete(ctx, &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cnsFileAccessConfigInstanceName,
			Namespace: c.supervisorNamespace,
		},
	}); err != nil {
		if errors.IsNotFound(err) {
			log.Infof("ControllerUnpublishVolume: CnsFileAccessConfig instance %q/%q already deleted. "+
				"Returning success for the detach operation", c.supervisorNamespace, cnsFileAccessConfigInstanceName)
			return &csi.ControllerUnpublishVolumeResponse{}, "", nil
		}
		msg := fmt.Sprintf("failed to delete CnsFileAccessConfig instance: %q/%q. Error: %+v",
			c.supervisorNamespace, cnsFileAccessConfigInstanceName, err)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
	}
	defer watchCnsFileAccessConfig.Stop()
	var cnsFileAccessConfigInstanceErr string
	isCnsFileAccessConfigInstanceDeleted := false
	// Watch all update events made on CnsFileAccessConfig instance until Deleted
	// event or a timeout occurs on the cnsfileaccessconfig instance.
	for !isCnsFileAccessConfigInstanceDeleted {
		log.Debugf("waiting for update on cnsfileaccessconfigs: %q", cnsFileAccessConfigInstanceName)
		event := <-watchCnsFileAccessConfig.ResultChan()
		cnsfileaccessconfig, ok := event.Object.(*cnsfileaccessconfigv1alpha1.CnsFileAccessConfig)
		if !ok {
			msg := fmt.Sprintf("Watch on cnsfileaccessconfig instance %q/%q timed out. Last seen error on the instance=%q",
				c.supervisorNamespace, cnsFileAccessConfigInstanceName, cnsFileAccessConfigInstanceErr)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		if cnsfileaccessconfig.Name != cnsFileAccessConfigInstanceName {
			log.Debugf("Observed CnsFileAccessConfig instance name: %q, expecting CnsFileAccessConfig instance name: %q",
				cnsfileaccessconfig.Name, cnsFileAccessConfigInstanceName)
			continue
		}
		// Check if SV PVC Name ain the cnsfileaccessconfig instance match with VolumeId from the request
		if cnsfileaccessconfig.Spec.PvcName != req.VolumeId {
			log.Debugf("Observed SV PVC Name: %q, expecting SV PVC Name: %q",
				cnsfileaccessconfig.Spec.PvcName, req.VolumeId)
			continue
		}
		// Check if VM name in the cnsfileaccessconfig instance match with NodeId from the request
		if cnsfileaccessconfig.Spec.VMName != req.NodeId {
			log.Debugf("Observed vm name: %q, expecting vm name: %q", cnsfileaccessconfig.Spec.VMName, req.NodeId)
			continue
		}
		if event.Type == "DELETED" {
			isCnsFileAccessConfigInstanceDeleted = true
		}
		cnsFileAccessConfigInstanceErr = cnsfileaccessconfig.Status.Error
	}
	log.Infof("ControllerUnpublishVolume: Volume detached successfully %q", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, "", nil
}

// ControllerExpandVolume expands a volume.
// volume id and size is retrieved from ControllerExpandVolumeRequest
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	start := time.Now()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	volumeType := prometheus.PrometheusUnknownVolumeType

	controllerExpandVolumeInternal := func() (
		*csi.ControllerExpandVolumeResponse, string, error) {
		log.Infof("ControllerExpandVolume: called with args %+v", req)
		// TODO: If the err is returned by invoking CNS API, then faultType should be
		// populated by the underlying layer.
		// If the request failed due to validate the request, "csi.fault.InvalidArgument" will be return.
		// If thr reqeust failed due to object not found, "csi.fault.NotFound" will be return.
		// For all other cases, the faultType will be set to "csi.fault.Internal" for now.
		// Later we may need to define different csi faults.

		err := validateGuestClusterControllerExpandVolumeRequest(ctx, req)
		if err != nil {
			return nil, csifault.CSIInvalidArgumentFault, err
		}
		// Only block volume expand is allowed. Update this when file volume expand is also supported.
		volumeType = prometheus.PrometheusBlockVolumeType

		volumeID := req.GetVolumeId()
		volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())

		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.OnlineVolumeExtend) {
			vmList, err := utils.ListVirtualMachines(ctx, c.vmOperatorClient, c.supervisorNamespace)
			if err != nil {
				msg := fmt.Sprintf("failed to list virtualmachines with error: %+v", err)
				log.Error(msg)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}

			for _, vmInstance := range vmList.Items {
				for _, vmVolume := range vmInstance.Status.Volumes {
					if vmVolume.Name == volumeID && vmVolume.Attached {
						msg := fmt.Sprintf("failed to expand volume: %q. Volume is attached to pod. "+
							"Only offline volume expansion is supported", volumeID)
						log.Error(msg)
						return nil, csifault.CSIInvalidArgumentFault, status.Error(codes.FailedPrecondition, msg)
					}
				}
			}
		}

		// Retrieve Supervisor PVC
		svPVC, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, volumeID, metav1.GetOptions{})
		if err != nil {
			msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v",
				volumeID, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}

		waitForSvPvcCondition := true
		gcPvcRequestSize := resource.NewQuantity(volSizeBytes, resource.Format(resource.BinarySI))
		svPvcRequestSize := svPVC.Spec.Resources.Requests[corev1.ResourceName(corev1.ResourceStorage)]
		// Check if GC PVC request size is greater than SV PVC request size
		switch (gcPvcRequestSize).Cmp(svPvcRequestSize) {
		case 1:
			// Update requested storage in SV PVC spec
			svPvcClone := svPVC.DeepCopy()
			svPvcClone.Spec.Resources.Requests[corev1.ResourceName(corev1.ResourceStorage)] = *gcPvcRequestSize

			// Make an update call to SV API server
			log.Infof("Increasing the size of supervisor PVC %s in namespace %s to %s",
				volumeID, c.supervisorNamespace, gcPvcRequestSize.String())
			svPVC, err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Update(
				ctx, svPvcClone, metav1.UpdateOptions{})
			if err != nil {
				msg := fmt.Sprintf("failed to update supervisor PVC %q in %q namespace. Error: %+v",
					volumeID, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}
		case 0:
			// GC PVC request size is equal to SV PVC request size
			log.Infof("Skipping resize call for supervisor PVC %s in namespace %s as it is already at the requested size",
				volumeID, c.supervisorNamespace)

			// SV PVC is already in FileSystemResizePending condition indicates
			// that SV PV has already been expanded to required size.
			if checkPVCCondition(ctx, svPVC, corev1.PersistentVolumeClaimFileSystemResizePending, gcPvcRequestSize) {
				waitForSvPvcCondition = false
			} else {
				// SV PVC is not in FileSystemResizePending condition and GC PVC request size is equal to SV PVC capacity
				// indicates that SV PVC is already at required size
				if (gcPvcRequestSize).Cmp(svPVC.Status.Capacity[corev1.ResourceName(corev1.ResourceStorage)]) == 0 {
					waitForSvPvcCondition = false
				}
			}
		default:
			// GC PVC request size is lesser than SV PVC request size
			msg := fmt.Sprintf("the requested size of the Supervisor PVC %s in namespace %s is %s "+
				"which is greater than the requested size of %s",
				volumeID, c.supervisorNamespace, svPvcRequestSize.String(), gcPvcRequestSize.String())
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.InvalidArgument, msg)
		}

		if waitForSvPvcCondition {
			// Wait for Supervisor PVC to change status to FilesystemResizePending
			err = checkForSupervisorPVCCondition(ctx, c.supervisorClient, svPVC,
				corev1.PersistentVolumeClaimFileSystemResizePending, gcPvcRequestSize,
				time.Duration(getResizeTimeoutInMin(ctx))*time.Minute)
			if err != nil {
				msg := fmt.Sprintf("failed to expand volume %s in namespace %s of supervisor cluster. Error: %+v",
					volumeID, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
			}
		}

		nodeExpansionRequired := true
		// Set NodeExpansionRequired to false for raw block volumes
		if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
			log.Infof("Node Expansion not supported for raw block volume ID %q in namespace %s of supervisor",
				volumeID, c.supervisorNamespace)
			nodeExpansionRequired = false
		}
		resp := &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         volSizeBytes,
			NodeExpansionRequired: nodeExpansionRequired,
		}
		return resp, "", nil
	}
	resp, faultType, err := controllerExpandVolumeInternal()
	log.Debugf("controllerExpandVolumeInternal: returns fault %q for volume %q", faultType, req.VolumeId)
	if err != nil {
		if csifault.IsNonStorageFault(faultType) {
			faultType = csifault.AddCsiNonStoragePrefix(ctx, faultType)
		}
		log.Errorf("Operation failed, reporting failure status to Prometheus."+
			" Operation Type: %q, Volume Type: %q, Fault Type: %q",
			prometheus.PrometheusExpandVolumeOpType, volumeType, faultType)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusExpandVolumeOpType,
			prometheus.PrometheusFailStatus, faultType).Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Volume %q expanded successfully.", req.VolumeId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusExpandVolumeOpType,
			prometheus.PrometheusPassStatus, faultType).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ValidateVolumeCapabilities returns the capabilities of the volume.
func (c *controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	log := logger.GetLogger(ctx)
	log.Infof("ValidateVolumeCapabilities: called with args %+v", req)
	volCaps := req.GetVolumeCapabilities()
	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	if err := common.IsValidVolumeCapabilities(ctx, volCaps); err == nil {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (c *controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {

	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ListVolumes: called with args %+v", req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetCapacity: called with args %+v", req)

	// Setting capacity to MaxInt64 for all topologies except for those which have been marked for deletion by VI Admin.
	totalcapacity := int64(math.MaxInt64)
	maxvolumesize := int64(math.MaxInt64)

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.WorkloadDomainIsolationFSS) {
		zonesMap := commonco.ContainerOrchestratorUtility.GetZonesForNamespace(c.supervisorNamespace)
		if req.AccessibleTopology != nil {
			if zonesMap == nil {
				return nil, logger.LogNewErrorCode(log, codes.Internal, "the guest cluster seems to be topology "+
					"aware but CSI controller could not find zone instances in supervisor cluster.")
			}

			segments := req.AccessibleTopology.GetSegments()
			if _, exists := zonesMap[segments["topology.kubernetes.io/zone"]]; !exists {
				totalcapacity = 0
				maxvolumesize = 0
				log.Infof("Zone %q is either marked for deletion or is not part of the namespace %q. "+
					"Setting capacity to 0.", segments["topology.kubernetes.io/zone"], c.supervisorNamespace)
			}
		} else {
			log.Debug("Not a topology aware guest cluster")
		}
	}

	return &csi.GetCapacityResponse{
		AvailableCapacity: totalcapacity,
		MaximumVolumeSize: &wrapperspb.Int64Value{Value: maxvolumesize},
	}, nil
}

func (c *controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetCapabilities: called with args %+v", req)
	var caps []*csi.ControllerServiceCapability
	for _, cap := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (c *controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (
	*csi.CreateSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType
	log.Infof("CreateSnapshot: called with args %+v", req)
	isBlockVolumeSnapshotWCPEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
		common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotWCPEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "createSnapshot")
	}
	createSnapshotInternal := func() (*csi.CreateSnapshotResponse, error) {
		// Search for supervisor PVC and ensure it exists
		supervisorPVCName := req.SourceVolumeId
		log.Infof("Checking if supervisor PVC %s/%s exists..", c.supervisorNamespace, supervisorPVCName)
		_, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			ctx, supervisorPVCName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				log.Errorf("the supervisor PVC: %s/%s was not found while attempting to take snapshot",
					c.supervisorNamespace, supervisorPVCName)
			}
			return nil, err
		}
		// Get supervisor VolumeSnapshotClass.
		var supervisorVolumeSnapshotClass string
		for param := range req.Parameters {
			paramName := strings.ToLower(param)
			if paramName == common.AttributeSupervisorVolumeSnapshotClass {
				supervisorVolumeSnapshotClass = req.Parameters[param]
			}
		}
		// Generate the supervisor VolumeSnapshot name
		// Assuming snapshot prefix is "snapshot-"
		supervisorVolumeSnapshotName := c.tanzukubernetesClusterUID + "-" + req.Name[9:]
		log.Infof("Determined VolumeSnapshotClass: %s for the supervisor VolumeSnapshot: %s",
			supervisorVolumeSnapshotClass, supervisorVolumeSnapshotName)
		log.Infof("Looking for VolumeSnapshot %s in supervisor namespace: %s ..",
			supervisorVolumeSnapshotName, c.supervisorNamespace)

		_, err = c.supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(c.supervisorNamespace).Get(
			ctx, supervisorVolumeSnapshotName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				// New createSnapshot request on the guest
				// Add "csi.vsphere.guest-initiated-csi-snapshot" annotation on VolumeSnapshot CR in
				// the supervisor cluster to indicate that snapshot creation is initiated from Guest cluster
				annotation := make(map[string]string)
				annotation[common.SupervisorVolumeSnapshotAnnotationKey] = "true"
				labels := make(map[string]string)
				finalizers := []string{}
				// Add CnsSnapshotFinalizer and TKC label to Supervisor snapshot
				// if SVPVCSnapshotProtectionFinalizer FSS is enabled
				if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx,
					common.SVPVCSnapshotProtectionFinalizer) {
					key := fmt.Sprintf("%s/%s", c.tanzukubernetesClusterName, c.guestClusterDist)
					labels[key] = c.tanzukubernetesClusterUID
					finalizers = append(finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
				}
				supVolumeSnapshot := constructVolumeSnapshotWithVolumeSnapshotClass(supervisorVolumeSnapshotName,
					c.supervisorNamespace, supervisorVolumeSnapshotClass, supervisorPVCName, annotation,
					labels, finalizers)
				log.Infof("Supervisosr VolumeSnapshot Spec: %+v", supVolumeSnapshot)
				_, err = c.supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(
					c.supervisorNamespace).Create(ctx, supVolumeSnapshot, metav1.CreateOptions{})
				if err != nil {
					msg := fmt.Sprintf("failed to create volumesnapshot with name: %s on namespace: %s "+
						"in supervisorCluster. Error: %+v", supervisorVolumeSnapshotName, c.supervisorNamespace, err)
					log.Error(msg)
					return nil, status.Error(codes.Internal, msg)
				}
				log.Infof("Successfully created VolumeSnapshot %s/%s on the supervisor cluster",
					c.supervisorNamespace, supervisorVolumeSnapshotName)
			} else {
				msg := fmt.Sprintf("failed to get volumesnapshot with name: %s on "+
					"namespace: %s in supervisorCluster. Error: %+v",
					supervisorVolumeSnapshotName, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
		}
		// Wait for VolumeSnapshot to be ready to use
		isReady, vs, err := common.IsVolumeSnapshotReady(ctx, c.supervisorSnapshotterClient,
			supervisorVolumeSnapshotName, c.supervisorNamespace,
			time.Duration(getSnapshotTimeoutInMin(ctx))*time.Minute)
		if !isReady {
			msg := fmt.Sprintf("volumesnapshot: %s on namespace: %s in supervisor cluster was not Ready. "+
				"Error: %+v", supervisorVolumeSnapshotName, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		// Extract the fcd-id + snapshot-id annotation from the supervisor volumesnapshot CR
		snapshotID := vs.Annotations[common.VolumeSnapshotInfoKey]
		volumeSnapshotName := req.Parameters[common.VolumeSnapshotNameKey]
		volumeSnapshotNamespace := req.Parameters[common.VolumeSnapshotNamespaceKey]

		log.Infof("Attempting to annotate Guest volumesnapshot %s/%s with %s",
			volumeSnapshotNamespace, volumeSnapshotName, snapshotID)
		annotated, err := commonco.ContainerOrchestratorUtility.AnnotateVolumeSnapshot(ctx, volumeSnapshotName,
			volumeSnapshotNamespace, map[string]string{common.VolumeSnapshotInfoKey: snapshotID})
		if err != nil || !annotated {
			log.Warnf("The snapshot: %s was created successfully, but failed to annotate volumesnapshot %s/%s"+
				"with annotation %s:%s. Error: %v", snapshotID, volumeSnapshotNamespace,
				volumeSnapshotName, common.VolumeSnapshotInfoKey, snapshotID, err)
		}
		snapshotCreateTimeInProto := timestamppb.New(vs.Status.CreationTime.Time)
		snapshotSize := vs.Status.RestoreSize.Value()
		createSnapshotResponse := &csi.CreateSnapshotResponse{
			Snapshot: &csi.Snapshot{
				SizeBytes:      snapshotSize,
				SnapshotId:     supervisorVolumeSnapshotName,
				SourceVolumeId: req.SourceVolumeId,
				CreationTime:   snapshotCreateTimeInProto,
				ReadyToUse:     true,
			},
		}
		return createSnapshotResponse, nil
	}

	resp, err := createSnapshotInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateSnapshotOpType,
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Snapshot for volume %q created successfully.", req.GetSourceVolumeId())
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusCreateSnapshotOpType,
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (c *controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType
	log.Infof("DeleteSnapshot: called with args %+v", req)
	isBlockVolumeSnapshotWCPEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotWCPEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "deleteSnapshot")
	}
	deleteSnapshotInternal := func() (*csi.DeleteSnapshotResponse, error) {
		csiSnapshotID := req.GetSnapshotId()
		// Retrieve the supervisor volumesnapshot
		supervisorVolumeSnapshotName := req.SnapshotId
		supervisorVolumeSnapshot, err := c.supervisorSnapshotterClient.SnapshotV1().
			VolumeSnapshots(c.supervisorNamespace).Get(ctx, supervisorVolumeSnapshotName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				log.Infof("The supervisor volumesnapshot %s/%s was not found in the supervisor cluster, "+
					"assuming successful delete", c.supervisorNamespace, supervisorVolumeSnapshotName)
			} else {
				msg := fmt.Sprintf("failed to retrieve the supervisor volumesnapshot %s/%s, Error: %+v",
					c.supervisorNamespace, supervisorVolumeSnapshotName, err)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
		}
		// Remove the finalizer before deleting the Supervisor VolumeSnapshot,
		// if SVPVCSnapshotProtectionFinalizer FSS is enabled
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SVPVCSnapshotProtectionFinalizer) {
			for i, finalizer := range supervisorVolumeSnapshot.ObjectMeta.Finalizers {
				if finalizer == cnsoperatortypes.CNSSnapshotFinalizer {
					log.Infof("Removing %q finalizer from VolumeSnapshot with name: %q on namespace: %q",
						cnsoperatortypes.CNSSnapshotFinalizer, supervisorVolumeSnapshotName, c.supervisorNamespace)
					supervisorVolumeSnapshot.ObjectMeta.Finalizers =
						slices.Delete(supervisorVolumeSnapshot.ObjectMeta.Finalizers, i, i+1)
					// Update the instance after removing finalizer
					_, err = c.supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(c.supervisorNamespace).
						Update(ctx, supervisorVolumeSnapshot, metav1.UpdateOptions{})
					if err != nil {
						msg := fmt.Sprintf("failed to update supervisor VolumeSnapshot %q in %q namespace. Error: %+v",
							supervisorVolumeSnapshotName, c.supervisorNamespace, err)
						log.Error(msg)
						return nil, status.Error(codes.Internal, msg)
					}
					break
				}
			}
		}
		log.Infof("Found the supervisor volumesnapshot %s/%s on the supervisor cluster",
			supervisorVolumeSnapshot.Namespace, supervisorVolumeSnapshot.Name)
		err = c.supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(c.supervisorNamespace).Delete(
			ctx, supervisorVolumeSnapshotName, *metav1.NewDeleteOptions(0))
		if err != nil {
			if errors.IsNotFound(err) {
				log.Infof("The supervisor volumesnapshot %s/%s was not found in the supervisor cluster "+
					"while deleting it, assuming successful delete",
					c.supervisorNamespace, supervisorVolumeSnapshotName)
				return &csi.DeleteSnapshotResponse{}, nil
			}
			msg := fmt.Sprintf("failed to delete the supervisor volumesnapshot %s/%s, Error: %+v",
				c.supervisorNamespace, supervisorVolumeSnapshotName, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}

		// Wait for VolumeSnapshot to be deleted from supervisor cluster
		err = common.WaitForVolumeSnapshotDeleted(ctx, c.supervisorSnapshotterClient,
			supervisorVolumeSnapshotName, c.supervisorNamespace,
			time.Duration(getSnapshotTimeoutInMin(ctx))*time.Minute)
		if err != nil {
			msg := fmt.Sprintf("volumeSnapshot: %s on namespace: %s in supervisor cluster was not deleted. "+
				"Error: %+v", supervisorVolumeSnapshotName, c.supervisorNamespace, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}

		log.Infof("DeleteSnapshot: successfully deleted snapshot %q", csiSnapshotID)
		return &csi.DeleteSnapshotResponse{}, nil
	}
	resp, err := deleteSnapshotInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteSnapshotOpType,
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		log.Infof("Snapshot %q deleted successfully.", req.SnapshotId)
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusDeleteSnapshotOpType,
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (c *controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	start := time.Now()
	volumeType := prometheus.PrometheusBlockVolumeType
	log.Infof("ListSnapshots: called with args %+v", req)
	isBlockVolumeSnapshotEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BlockVolumeSnapshot)
	if !isBlockVolumeSnapshotEnabled {
		return nil, logger.LogNewErrorCode(log, codes.Unimplemented, "listSnapshot")
	}
	listSnapshotsInternal := func() (*csi.ListSnapshotsResponse, error) {
		log.Infof("ListSnapshots: called with args %+v", req)
		maxEntries := common.QuerySnapshotLimit
		if req.MaxEntries != 0 {
			log.Warnf("Specifying MaxEntries in ListSnapshotRequest is not supported,"+
				" will return %d entries", maxEntries)
			// TODO: Support specifying max entries when result pagination is supported.
			// maxEntries = int64(req.MaxEntries)
		}
		log.Infof("Setting the max entries to %d", maxEntries)
		// Within the Guest:
		// snapshotID: supervisor volumesnapshot name
		// volumeID: supervisor PVC name
		snapshotID := req.SnapshotId
		volumeID := req.SourceVolumeId
		var entries []*csi.ListSnapshotsResponse_Entry
		if snapshotID != "" {
			vs, err := c.supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(c.supervisorNamespace).Get(
				ctx, snapshotID, metav1.GetOptions{})
			if err != nil {
				msg := fmt.Sprintf("failed to get volumesnapshot with name: %s on namespace: %s from "+
					"Supervisor Cluster. Error: %+v", snapshotID, c.supervisorNamespace, err)
				log.Error(msg)
				return nil, status.Error(codes.Internal, msg)
			}
			entry := constructListSnapshotEntry(*vs)
			entries = append(entries, entry)
		} else if volumeID != "" {
			// Retrieve all the snapshots for the specific volume
			vsList, err := c.supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(c.supervisorNamespace).
				List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve all the volumesnapshot objects from supervisor cluster %s", c.supervisorNamespace)
			}
			for _, vs := range vsList.Items {
				supPVCName := *vs.Spec.Source.PersistentVolumeClaimName
				if supPVCName == volumeID {
					entry := constructListSnapshotEntry(vs)
					entries = append(entries, entry)
				}
			}
		} else {
			vsList, err := c.supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(c.supervisorNamespace).
				List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to retrieve all the volumesnapshot objects from supervisor cluster %s", c.supervisorNamespace)
			}
			for _, vs := range vsList.Items {
				entry := constructListSnapshotEntry(vs)
				entries = append(entries, entry)
			}
		}
		resp := &csi.ListSnapshotsResponse{
			Entries: entries,
		}
		return resp, nil
	}
	resp, err := listSnapshotsInternal()
	if err != nil {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusFailStatus, "NotComputed").Observe(time.Since(start).Seconds())
	} else {
		prometheus.CsiControlOpsHistVec.WithLabelValues(volumeType, prometheus.PrometheusListSnapshotsOpType,
			prometheus.PrometheusPassStatus, "").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (c *controller) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (
	*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (
	*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
