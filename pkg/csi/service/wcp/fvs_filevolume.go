/*
Copyright 2026 The Kubernetes Authors.

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

package wcp

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fvv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/filevolume/v1alpha1"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	fvsGroup   = "fvs.vcf.broadcom.com"
	fvsVersion = "v1alpha1"
	// fvsWaitStep is the poll interval while waiting for the FileVolume CR to reach Ready and populate
	// status (FVS controllers reconcile asynchronously; we re-read on this cadence instead of hammering
	// the API).
	fvsWaitStep = 5 * time.Second
	// fvsWaitMax is the maximum time to wait before failing CreateVolume so the CSI RPC does not block indefinitely
	// if the FileVolume never becomes Ready or status fields stay empty.
	fvsWaitMax = 5 * time.Minute
	// AnnotationVPCNetworkConfig is set on supervisor namespaces; value is the VPCNetworkConfiguration CR name.
	AnnotationVPCNetworkConfig = "nsx.vmware.com/vpc_network_config"
)

var (
	vpcNetworkConfigurationGVR = schema.GroupVersionResource{
		Group:    "crd.nsx.vmware.com",
		Version:  "v1alpha1",
		Resource: "vpcnetworkconfigurations",
	}
	fvsFileVolumeGVR = schema.GroupVersionResource{
		Group:    fvsGroup,
		Version:  fvsVersion,
		Resource: "filevolumes",
	}
	fvsFileVolumeServiceGVR = schema.GroupVersionResource{
		Group:    fvsGroup,
		Version:  fvsVersion,
		Resource: "filevolumeservices",
	}
)

// fvsZonesForNamespace resolves zone labels for a supervisor namespace for FVS filtering and topology.
// Unit tests may replace this hook; production uses the container orchestrator.
var fvsZonesForNamespace = func(ns string) map[string]struct{} {
	return commonco.ContainerOrchestratorUtility.GetZonesForNamespace(ns)
}

// isVsanFileServicePolicyStorageClass returns true for supervisor storage classes that route
// volume provisioning through the FVS FileVolume CR workflow when NSX_VPC and capability match.
func isVsanFileServicePolicyStorageClass(storageClassName string) bool {
	switch storageClassName {
	case common.StorageClassVsanFileServicePolicy,
		common.StorageClassVsanFileServicePolicyLateBinding:
		return true
	default:
		return false
	}
}

// vpcPathFromVPCNetworkConfiguration returns status.vpcs[0].vpcPath from a VPCNetworkConfiguration CR
// (crd.nsx.vmware.com/v1alpha1). VPC matching uses this path, not status.vpcs[0].name.
func vpcPathFromVPCNetworkConfiguration(obj *unstructured.Unstructured) string {
	vpcs, found, err := unstructured.NestedSlice(obj.Object, "status", "vpcs")
	if err == nil && found && len(vpcs) > 0 {
		if vpc, ok := vpcs[0].(map[string]interface{}); ok {
			if path, ok := vpc["vpcPath"].(string); ok && path != "" {
				return path
			}
		}
	}
	return ""
}

// findVPCNetworkConfigurationForNamespace loads the supervisor Namespace, reads the
// AnnotationVPCNetworkConfig annotation for the VPCNetworkConfiguration object name, and returns that
// cluster-scoped VPCNetworkConfiguration CR (crd.nsx.vmware.com/v1alpha1).
func (c *controller) findVPCNetworkConfigurationForNamespace(ctx context.Context, namespace string) (
	*unstructured.Unstructured, error) {
	if c.namespaceLister == nil {
		return nil, fmt.Errorf("namespace lister is not initialized")
	}
	ns, err := c.namespaceLister.Get(namespace)
	if err != nil {
		return nil, fmt.Errorf("get namespace %q from informer cache: %w", namespace, err)
	}
	crName := ns.Annotations[AnnotationVPCNetworkConfig]
	if crName == "" {
		return nil, fmt.Errorf("namespace %q has no %q annotation", namespace, AnnotationVPCNetworkConfig)
	}
	cr, err := c.dynamicClient.Resource(vpcNetworkConfigurationGVR).Get(ctx, crName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get VPCNetworkConfiguration %q: %w", crName, err)
	}
	return cr, nil
}

// getVPCPathForNamespace returns status.vpcs[0].vpcPath for a supervisor namespace from its
// VPCNetworkConfiguration CR referenced by the namespace annotation.
func (c *controller) getVPCPathForNamespace(ctx context.Context, namespace string) (string, error) {
	cr, err := c.findVPCNetworkConfigurationForNamespace(ctx, namespace)
	if err != nil {
		return "", err
	}
	p := vpcPathFromVPCNetworkConfiguration(cr)
	if p == "" {
		return "", fmt.Errorf("no VPC path in status.vpcs for VPCNetworkConfiguration %q", cr.GetName())
	}
	return p, nil
}

// listFVSCandidateInstanceNamespaces returns supervisor namespaces that may host the FVS FileVolume CR:
// same VPC path as the consumer (status.vpcs[0].vpcPath on VPCNetworkConfiguration), excluding the PVC namespace,
// and with at least one requested zone present on the namespace (zone CRs).
// Each namespace must have AnnotationVPCNetworkConfig pointing at its cluster-scoped VPCNetworkConfiguration;
// namespaces without the annotation are skipped.
func (c *controller) listFVSCandidateInstanceNamespaces(ctx context.Context, pvcNamespace string,
	requestedZones []string) ([]string, error) {
	log := logger.GetLogger(ctx)
	if len(requestedZones) == 0 {
		return nil, fmt.Errorf("requested zones must be non-empty to select FVS instance namespaces")
	}

	vpcPath, err := c.getVPCPathForNamespace(ctx, pvcNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve VPC path for namespace %q: %w", pvcNamespace, err)
	}
	log.Infof("listFVSCandidateInstanceNamespaces: consumer namespace %q uses VPC path %q, requested zones %v",
		pvcNamespace, vpcPath, requestedZones)

	if c.namespaceLister == nil {
		return nil, fmt.Errorf("namespace lister is not initialized")
	}
	// TODO: FileVolumeService: filter namespaces by labels for instance namespaces
	nsObjs, err := c.namespaceLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list namespaces from informer cache: %w", err)
	}

	var candidateNamespaces []string
	for _, ns := range nsObjs {
		if ns.Name == pvcNamespace {
			continue
		}
		crName := ns.Annotations[AnnotationVPCNetworkConfig]
		if crName == "" {
			log.Debugf("listFVSCandidateInstanceNamespaces: skipping namespace %q: missing %q annotation",
				ns.Name, AnnotationVPCNetworkConfig)
			continue
		}
		cr, err := c.dynamicClient.Resource(vpcNetworkConfigurationGVR).Get(ctx, crName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get VPCNetworkConfiguration %q for namespace %q: %w", crName, ns.Name, err)
		}
		nsVPCPath := vpcPathFromVPCNetworkConfiguration(cr)
		if nsVPCPath != vpcPath {
			continue
		}
		log.Infof("listFVSCandidateInstanceNamespaces: namespace %q matches consumer VPC path %q "+
			"(VPCNetworkConfiguration %q)",
			ns.Name, vpcPath, crName)
		if !namespaceHasAnyRequestedZone(ns.Name, requestedZones) {
			log.Infof("listFVSCandidateInstanceNamespaces: skipping namespace %q: "+
				"no overlap with requested zones %v",
				ns.Name, requestedZones)
			continue
		}
		log.Infof("listFVSCandidateInstanceNamespaces: adding namespace %q to FVS candidate list "+
			"(VPC path %q, requested zones %v)",
			ns.Name, vpcPath, requestedZones)
		candidateNamespaces = append(candidateNamespaces, ns.Name)
	}
	if len(candidateNamespaces) == 0 {
		return nil, fmt.Errorf("no FVS candidate namespace for VPC path %q and zones %v (consumer namespace %q)",
			vpcPath, requestedZones, pvcNamespace)
	}
	return candidateNamespaces, nil
}

// instanceNamespaceHasReadyFileVolumeService returns nil if the namespace has at least one
// FileVolumeService (fvs.vcf.broadcom.com/v1alpha1) whose status.healthState is Ready (case-insensitive).
// Otherwise it returns an error describing a missing list, no CRs, or no healthy service.
func (c *controller) instanceNamespaceHasReadyFileVolumeService(ctx context.Context, instanceNS string) error {
	list, err := c.dynamicClient.Resource(fvsFileVolumeServiceGVR).Namespace(instanceNS).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	if len(list.Items) == 0 {
		return fmt.Errorf("no FileVolumeService in namespace %q", instanceNS)
	}
	for _, item := range list.Items {
		obj := item.Object
		if hs, found, _ := unstructured.NestedString(obj, "status", "healthState"); found &&
			strings.EqualFold(hs, "Ready") {
			return nil
		}
	}
	return fmt.Errorf("no healthy FileVolumeService in namespace %q", instanceNS)
}

// namespaceHasAnyRequestedZone reports whether the supervisor namespace is associated with at least one
// of the topology zones requested for the volume (via ContainerOrchestratorUtility zone CRs).
func namespaceHasAnyRequestedZone(namespace string, requestedZones []string) bool {
	if len(requestedZones) == 0 {
		return true
	}
	zonesInNS := fvsZonesForNamespace(namespace)
	for _, z := range requestedZones {
		if _, ok := zonesInNS[z]; ok {
			return true
		}
	}
	return false
}

// topologyListFromZoneMap builds a CSI Topology list from a set of zone names, with each entry
// labeling topology.kubernetes.io/zone to that zone value.
func topologyListFromZoneMap(zones map[string]struct{}) []*csi.Topology {
	var out []*csi.Topology
	for z := range zones {
		out = append(out, &csi.Topology{
			Segments: map[string]string{v1.LabelTopologyZone: z},
		})
	}
	return out
}

// fvsAccessibleTopology sets CSI AccessibleTopology from zone assignments on the PVC (workload)
// namespace and the instance namespace (where the FileVolume CR runs).
//
// If the volume is placed where the PVC namespace and instance namespace share at least one zone (the
// requested topology aligns with a zone assigned to the PVC namespace and that zone is also on the
// instance namespace), those shared zones are set as AccessibleTopology for the volume.
//
// If the two namespaces share no zone (including when the volume would not sit on overlapping namespace
// zones), all zones assigned to the PVC namespace are set as AccessibleTopology for the volume.
func fvsAccessibleTopology(pvcNamespace, instanceNS string) ([]*csi.Topology, error) {
	pvcNamespaceZones := fvsZonesForNamespace(pvcNamespace)
	if len(pvcNamespaceZones) == 0 {
		return nil, fmt.Errorf("no zones assigned to PVC namespace %q", pvcNamespace)
	}
	instanceNamespaceZones := fvsZonesForNamespace(instanceNS)
	if len(instanceNamespaceZones) == 0 {
		return nil, fmt.Errorf("no zones assigned to instance namespace %q", instanceNS)
	}

	shared := make(map[string]struct{})
	for z := range instanceNamespaceZones {
		if _, ok := pvcNamespaceZones[z]; ok {
			shared[z] = struct{}{}
		}
	}
	if len(shared) == 0 {
		return topologyListFromZoneMap(pvcNamespaceZones), nil
	}
	return topologyListFromZoneMap(shared), nil
}

// createFileVolumeViaFVS provisions a file volume by creating a FileVolume CR in the FVS instance namespace.
func (c *controller) createFileVolumeViaFVS(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, string, error) {
	log := logger.GetLogger(ctx)
	pvcNamespace := req.Parameters[common.AttributePvcNamespace]
	pvcName := req.Parameters[common.AttributePvcName]
	if pvcNamespace == "" || pvcName == "" {
		return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"missing PVC name or namespace in CreateVolume parameters")
	}

	if req.GetAccessibilityRequirements() == nil {
		return nil, csifault.CSIInvalidArgumentFault, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"accessibility requirements are required for FVS file volume provisioning")
	}

	pvcUID, err := common.ExtractVolumeIDFromPVName(ctx, req.GetName())
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to extract PVC UID from volume name %q: %v", req.GetName(), err)
	}
	fvName := pvcUID

	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	qty := resource.NewQuantity(volSizeBytes, resource.BinarySI)

	requestedZones, err := GetZonesFromAccessibilityRequirements(ctx, req.GetAccessibilityRequirements())
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to read zones from accessibility requirements: %v", err)
	}

	candidateNamespaces, err := c.listFVSCandidateInstanceNamespaces(ctx, pvcNamespace, requestedZones)
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"%v", err)
	}

	// Phase 1: see if a FileVolume CR with this name (PVC UID) already exists in any candidate instance namespace.
	var instanceNS string
	for _, ns := range candidateNamespaces {
		fvExisting := &fvv1alpha1.FileVolume{}
		getErr := c.fileVolumeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: ns, Name: fvName}, fvExisting)
		if getErr == nil {
			instanceNS = ns
			log.Infof("reusing existing FileVolume %s/%s for PVC %s/%s", ns, fvName, pvcNamespace, pvcName)
			break
		}
		if !apierrors.IsNotFound(getErr) {
			log.Warnf("could not get FileVolume %q in namespace %q (will try next candidate): %v",
				fvName, ns, getErr)
			continue
		}
	}

	// Phase 2: no existing FileVolume — pick one candidate namespace and create the CR.
	if instanceNS == "" {
		var targetNS string
		// Randomize order so repeated creates do not always probe the same instance namespace first (hotspot).
		rand.Shuffle(len(candidateNamespaces), func(i, j int) {
			candidateNamespaces[i], candidateNamespaces[j] = candidateNamespaces[j], candidateNamespaces[i]
		})
		for _, ns := range candidateNamespaces {
			if svcErr := c.instanceNamespaceHasReadyFileVolumeService(ctx, ns); svcErr != nil {
				log.Debugf("skipping instance namespace %q for FileVolume create: %v", ns, svcErr)
				continue
			}
			targetNS = ns
			break
		}
		if targetNS == "" {
			return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"no instance namespace with a healthy FileVolumeService for requested zones %v", requestedZones)
		}

		fvTyped := &fvv1alpha1.FileVolume{
			TypeMeta: metav1.TypeMeta{
				APIVersion: schema.GroupVersion{Group: fvsGroup, Version: fvsVersion}.String(),
				Kind:       "FileVolume",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fvName,
				Namespace: targetNS,
			},
			Spec: fvv1alpha1.FileVolumeSpec{
				PvcUID:    pvcUID,
				Size:      *qty,
				Protocols: []fvv1alpha1.FileVolumeProtocol{fvv1alpha1.FileVolumeProtocolNFSv4},
			},
		}
		log.Infof("Creating FileVolume CR for PVC %s/%s: %+v", pvcNamespace, pvcName, fvTyped)
		createErr := c.fileVolumeClient.Create(ctx, fvTyped)
		if createErr != nil {
			if !apierrors.IsAlreadyExists(createErr) {
				return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to create FileVolume %s/%s: %v", targetNS, fvName, createErr)
			}
			log.Infof("FileVolume %s/%s already exists, monitoring status for PVC %s/%s",
				targetNS, fvName, pvcNamespace, pvcName)
		} else {
			log.Infof("Created FileVolume %s/%s for PVC %s/%s", targetNS, fvName, pvcNamespace, pvcName)
		}
		instanceNS = targetNS
	}

	fv := &fvv1alpha1.FileVolume{}
	var exportPath, endpoint string
	err = wait.PollUntilContextTimeout(ctx, fvsWaitStep, fvsWaitMax, true, func(ctx context.Context) (bool, error) {
		if err := c.fileVolumeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: instanceNS, Name: fvName}, fv); err != nil {
			return false, err
		}
		phase := fv.Status.Phase
		if phase == "" {
			return false, nil
		}
		if strings.EqualFold(string(phase), string(fvv1alpha1.FileVolumePhaseError)) {
			return false, fmt.Errorf("FileVolume %s/%s entered Error phase", instanceNS, fvName)
		}
		if !strings.EqualFold(string(phase), string(fvv1alpha1.FileVolumePhaseReady)) {
			return false, nil
		}
		exportPath = fv.Status.ExportPath
		endpoint = fv.Status.Endpoint
		return exportPath != "" && endpoint != "", nil
	})
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.DeadlineExceeded,
			"timeout or error waiting for FileVolume %s/%s to become Ready with export path and endpoint: %v",
			instanceNS, fvName, err)
	}

	volumeID := fmt.Sprintf("%s%s:%s", common.FVSVolumeIDPrefix, instanceNS, fvName)
	nfsv41Export := endpoint + ":" + exportPath
	attributes := map[string]string{
		common.AttributeDiskType:            common.DiskTypeFileVolume,
		common.Nfsv4ExportPathAnnotationKey: nfsv41Export,
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: volSizeBytes,
			VolumeContext: attributes,
		},
	}

	topo, err := fvsAccessibleTopology(pvcNamespace, instanceNS)
	if err != nil {
		return nil, csifault.CSIInternalFault, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to compute accessible topology: %v", err)
	}
	resp.Volume.AccessibleTopology = topo

	return resp, "", nil
}

// shouldProvisionVsanFileVolumeViaFVS encapsulates routing to the FVS CR workflow.
func shouldProvisionVsanFileVolumeViaFVS(ctx context.Context, storageClassName string) (bool, error) {
	if !isVsanFileServicePolicyStorageClass(storageClassName) {
		return false, nil
	}
	np, err := cnsoperatorutil.GetNetworkProvider(ctx)
	if err != nil {
		return false, err
	}
	if np != cnsoperatorutil.VPCNetworkProvider {
		return false, status.Errorf(codes.FailedPrecondition,
			"storage class %q requires NSX_VPC network provider (current network provider: %q)",
			storageClassName, np)
	}
	if !isVsanFileVolumeServiceFSSEnabled {
		return false, nil
	}
	return true, nil
}
