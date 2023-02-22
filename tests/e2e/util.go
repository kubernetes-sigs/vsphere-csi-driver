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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-version"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v2"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	pkgtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/drain"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	"k8s.io/kubernetes/test/e2e/framework/manifest"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"

	snapc "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var (
	defaultCluster         *object.ClusterComputeResource
	svcClient              clientset.Interface
	svcNamespace           string
	vsanHealthClient       *VsanClient
	clusterComputeResource []*object.ClusterComputeResource
	hosts                  []*object.HostSystem
	defaultDatastore       *object.Datastore
	restConfig             *rest.Config
	pvclaims               []*v1.PersistentVolumeClaim
	pvclaimsToDelete       []*v1.PersistentVolumeClaim
	pvZone                 string
	pvRegion               string
)

type TKGCluster struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name            string `yaml:"name"`
		Namespace       string `yaml:"namespace"`
		ResourceVersion string `yaml:"resourceVersion"`
		SelfLink        string `yaml:"selfLink"`
		UID             string `yaml:"uid"`
	} `yaml:"metadata"`
	Spec struct {
		Distribution struct {
			FullVersion string `yaml:"fullVersion"`
			Version     string `yaml:"version"`
		} `yaml:"distribution"`
		Settings struct {
			Network struct {
				Cni struct {
					Name string `yaml:"name"`
				} `yaml:"cni"`
				Pods struct {
					CidrBlocks []string `yaml:"cidrBlocks"`
				} `yaml:"pods"`
				ServiceDomain string `yaml:"serviceDomain"`
				Services      struct {
					CidrBlocks []string `yaml:"cidrBlocks"`
				} `yaml:"services"`
			} `yaml:"network"`
		} `yaml:"settings"`
		Topology struct {
			ControlPlane struct {
				Class        string `yaml:"class"`
				Count        int    `yaml:"count"`
				StorageClass string `yaml:"storageClass"`
			} `yaml:"controlPlane"`
			Workers struct {
				Class        string `yaml:"class"`
				Count        int    `yaml:"count"`
				StorageClass string `yaml:"storageClass"`
			} `yaml:"workers"`
		} `yaml:"topology"`
	} `yaml:"spec"`
	Status struct {
		Addons struct {
			Authsvc struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name    string `yaml:"name"`
				Status  string `yaml:"status"`
				Version string `yaml:"version"`
			} `yaml:"authsvc"`
			Cloudprovider struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name    string `yaml:"name"`
				Status  string `yaml:"status"`
				Version string `yaml:"version"`
			} `yaml:"cloudprovider"`
			Cni struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name    string `yaml:"name"`
				Status  string `yaml:"status"`
				Version string `yaml:"version"`
			} `yaml:"cni"`
			Csi struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name    string `yaml:"name"`
				Status  string `yaml:"status"`
				Version string `yaml:"version"`
			} `yaml:"csi"`
			DNS struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name    string `yaml:"name"`
				Status  string `yaml:"status"`
				Version string `yaml:"version"`
			} `yaml:"dns"`
			MetricsServer struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Reason             string    `yaml:"reason"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name   string `yaml:"name"`
				Status string `yaml:"status"`
			} `yaml:"metrics-server"`
			Proxy struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name    string `yaml:"name"`
				Status  string `yaml:"status"`
				Version string `yaml:"version"`
			} `yaml:"proxy"`
			Psp struct {
				Conditions []struct {
					LastTransitionTime time.Time `yaml:"lastTransitionTime"`
					Status             string    `yaml:"status"`
					Type               string    `yaml:"type"`
				} `yaml:"conditions"`
				Name    string `yaml:"name"`
				Status  string `yaml:"status"`
				Version string `yaml:"version"`
			} `yaml:"psp"`
		} `yaml:"addons"`
		ClusterAPIStatus struct {
			APIEndpoints []struct {
				Host string `yaml:"host"`
				Port int    `yaml:"port"`
			} `yaml:"apiEndpoints"`
		} `yaml:"clusterApiStatus"`
		Conditions []struct {
			LastTransitionTime time.Time `yaml:"lastTransitionTime"`
			Status             string    `yaml:"status"`
			Type               string    `yaml:"type"`
			Message            string    `yaml:"message,omitempty"`
			Reason             string    `yaml:"reason,omitempty"`
		} `yaml:"conditions"`
		Phase string `yaml:"phase"`
	} `yaml:"status"`
}

type VMImages struct {
	APIVersion string `json:"apiVersion"`
	Items      []struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Annotations struct {
				VmoperatorVmwareComContentLibraryVersion  string `json:"vmoperator.vmware.com/content-library-version"`
				VmwareSystemCompatibilityoffering         string `json:"vmware-system.compatibilityoffering"`
				VmwareSystemGuestKubernetesAddonsAntrea   string `json:"vmware-system.guest.kubernetes.addons.antrea"`
				VmwareSystemGuestKubernetesAddonsAuthsvc  string `json:"vmware-system.guest.kubernetes.addons.authsvc"`
				VmwareSystemGuestKubernetesAddonsCalico   string `json:"vmware-system.guest.kubernetes.addons.calico"`
				VmwareSystemGuestKubernetesAddonsPvcsi    string `json:"vmware-system.guest.kubernetes.addons.pvcsi"`
				VmwareSystemGuestKubernetesAddonsVmwareGC string `json:"vmware-system.guest.kubernetes.addons.vmware-guest-cluster"`
				VmwareSystemGuestKubernetesImageVersion   string `json:"vmware-system.guest.kubernetes.distribution.image.version"`
			} `json:"annotations"`
			CreationTimestamp time.Time `json:"creationTimestamp"`
			Generation        int       `json:"generation"`
			ManagedFields     []struct {
				APIVersion string `json:"apiVersion"`
				FieldsType string `json:"fieldsType"`
				FieldsV1   struct {
					FMetadata struct {
						FAnnotations struct {
							NAMING_FAILED struct {
							} `json:"."`
							FVmoperatorVmwareComContentLibraryVersion struct {
							} `json:"f:vmoperator.vmware.com/content-library-version"`
							FVmwareSystemCompatibilityoffering struct {
							} `json:"f:vmware-system.compatibilityoffering"`
							FVmwareSystemGuestKubernetesAddonsAntrea struct {
							} `json:"f:vmware-system.guest.kubernetes.addons.antrea"`
							FVmwareSystemGuestKubernetesAddonsAuthsvc struct {
							} `json:"f:vmware-system.guest.kubernetes.addons.authsvc"`
							FVmwareSystemGuestKubernetesAddonsCalico struct {
							} `json:"f:vmware-system.guest.kubernetes.addons.calico"`
							FVmwareSystemGuestKubernetesAddonsPvcsi struct {
							} `json:"f:vmware-system.guest.kubernetes.addons.pvcsi"`
							FVmwareSystemGuestKubernetesAddonsVmwareGC struct {
							} `json:"f:vmware-system.guest.kubernetes.addons.vmware-guest-cluster"`
							FVmwareSystemGuestKubernetesImageVersion struct {
							} `json:"f:vmware-system.guest.kubernetes.distribution.image.version"`
						} `json:"f:annotations"`
					} `json:"f:metadata"`
					FSpec struct {
						NAMING_FAILED struct {
						} `json:"."`
						FHwVersion struct {
						} `json:"f:hwVersion"`
						FImageSourceType struct {
						} `json:"f:imageSourceType"`
						FOsInfo struct {
							NAMING_FAILED struct {
							} `json:"."`
							FType struct {
							} `json:"f:type"`
						} `json:"f:osInfo"`
						FProductInfo struct {
							NAMING_FAILED struct {
							} `json:"."`
							FFullVersion struct {
							} `json:"f:fullVersion"`
							FProduct struct {
							} `json:"f:product"`
							FVendor struct {
							} `json:"f:vendor"`
							FVersion struct {
							} `json:"f:version"`
						} `json:"f:productInfo"`
						FType struct {
						} `json:"f:type"`
					} `json:"f:spec"`
					FStatus struct {
						NAMING_FAILED struct {
						} `json:"."`
						FConditions struct {
						} `json:"f:conditions"`
						FImageSupported struct {
						} `json:"f:imageSupported"`
						FInternalID struct {
						} `json:"f:internalId"`
						FUUID struct {
						} `json:"f:uuid"`
					} `json:"f:status"`
				} `json:"fieldsV1"`
				Manager   string    `json:"manager"`
				Operation string    `json:"operation"`
				Time      time.Time `json:"time"`
			} `json:"managedFields"`
			Name            string `json:"name"`
			OwnerReferences []struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Name       string `json:"name"`
				UID        string `json:"uid"`
			} `json:"ownerReferences"`
			ResourceVersion string `json:"resourceVersion"`
			SelfLink        string `json:"selfLink"`
			UID             string `json:"uid"`
		} `json:"metadata"`
		Spec struct {
			HwVersion       int    `json:"hwVersion"`
			ImageSourceType string `json:"imageSourceType"`
			OsInfo          struct {
				Type string `json:"type"`
			} `json:"osInfo"`
			ProductInfo struct {
				FullVersion string `json:"fullVersion"`
				Product     string `json:"product"`
				Vendor      string `json:"vendor"`
				Version     string `json:"version"`
			} `json:"productInfo"`
			Type string `json:"type"`
		} `json:"spec"`
		Status struct {
			Conditions []struct {
				LastTransitionTime time.Time `json:"lastTransitionTime"`
				Status             string    `json:"status"`
				Type               string    `json:"type"`
			} `json:"conditions"`
			ImageSupported bool   `json:"imageSupported"`
			InternalID     string `json:"internalId"`
			UUID           string `json:"uuid"`
		} `json:"status"`
	} `json:"items"`
	Kind     string `json:"kind"`
	Metadata struct {
		Continue        string `json:"continue"`
		ResourceVersion string `json:"resourceVersion"`
		SelfLink        string `json:"selfLink"`
	} `json:"metadata"`
}

type AuthToken struct {
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type CertRotate struct {
	UserID                     string        `json:"user_id"`
	UserName                   string        `json:"user_name"`
	Created                    time.Time     `json:"created"`
	Version                    int           `json:"version"`
	ID                         string        `json:"id"`
	UpdatedByUserID            string        `json:"updated_by_user_id"`
	UpdatedByUserName          string        `json:"updated_by_user_name"`
	Updated                    time.Time     `json:"updated"`
	Status                     string        `json:"status"`
	ResourceID                 string        `json:"resource_id"`
	ResourceType               string        `json:"resource_type"`
	StartResourceEntityVersion interface{}   `json:"start_resource_entity_version"`
	EndResourceEntityVersion   interface{}   `json:"end_resource_entity_version"`
	ParentTaskID               interface{}   `json:"parent_task_id"`
	SubStatus                  string        `json:"sub_status"`
	TaskType                   string        `json:"task_type"`
	ErrorMessage               interface{}   `json:"error_message"`
	CustomerErrorMessage       interface{}   `json:"customer_error_message"`
	LocalizedErrorMessage      interface{}   `json:"localized_error_message"`
	StartTime                  time.Time     `json:"start_time"`
	EndTime                    interface{}   `json:"end_time"`
	Retries                    int           `json:"retries"`
	TaskVersion                string        `json:"task_version"`
	ProgressPercent            int           `json:"progress_percent"`
	EstimatedRemainingMinutes  int           `json:"estimated_remaining_minutes"`
	TaskProgressPhases         interface{}   `json:"task_progress_phases"`
	ServiceErrors              []interface{} `json:"service_errors"`
	Params                     struct {
		CertType struct {
			CertType            string      `json:"cert_type"`
			CertificateToDelete interface{} `json:"certificate_to_delete"`
			CertificateToSave   interface{} `json:"certificate_to_save"`
			ValidDays           interface{} `json:"valid_days"`
			Version             interface{} `json:"version"`
			CertificateConfig   interface{} `json:"certificate_config"`
		} `json:"certType"`
	} `json:"params"`
	OrgType         string      `json:"org_type"`
	CorrelationID   interface{} `json:"correlation_id"`
	PhaseInProgress string      `json:"phase_in_progress"`
	OrgID           string      `json:"org_id"`
}

type GetTaskTstatus struct {
	UserID                     string        `json:"user_id"`
	UserName                   string        `json:"user_name"`
	Created                    time.Time     `json:"created"`
	Version                    int           `json:"version"`
	ID                         string        `json:"id"`
	UpdatedByUserID            string        `json:"updated_by_user_id"`
	UpdatedByUserName          string        `json:"updated_by_user_name"`
	Updated                    time.Time     `json:"updated"`
	Status                     string        `json:"status"`
	ResourceID                 string        `json:"resource_id"`
	ResourceType               string        `json:"resource_type"`
	StartResourceEntityVersion interface{}   `json:"start_resource_entity_version"`
	EndResourceEntityVersion   interface{}   `json:"end_resource_entity_version"`
	ParentTaskID               interface{}   `json:"parent_task_id"`
	SubStatus                  string        `json:"sub_status"`
	TaskType                   string        `json:"task_type"`
	ErrorMessage               interface{}   `json:"error_message"`
	CustomerErrorMessage       interface{}   `json:"customer_error_message"`
	LocalizedErrorMessage      interface{}   `json:"localized_error_message"`
	StartTime                  time.Time     `json:"start_time"`
	EndTime                    time.Time     `json:"end_time"`
	Retries                    int           `json:"retries"`
	TaskVersion                string        `json:"task_version"`
	ProgressPercent            int           `json:"progress_percent"`
	EstimatedRemainingMinutes  int           `json:"estimated_remaining_minutes"`
	TaskProgressPhases         interface{}   `json:"task_progress_phases"`
	ServiceErrors              []interface{} `json:"service_errors"`
	Params                     struct {
		CertType struct {
			CertType            string `json:"cert_type"`
			CertificateToDelete []struct {
				Entity      string `json:"entity"`
				SerialNum   string `json:"serial_num"`
				Fingerprint string `json:"fingerprint"`
				Certificate string `json:"certificate"`
			} `json:"certificate_to_delete"`
			CertificateToSave []struct {
				Entity      string `json:"entity"`
				SerialNum   string `json:"serial_num"`
				Fingerprint string `json:"fingerprint"`
				Certificate string `json:"certificate"`
			} `json:"certificate_to_save"`
		} `json:"certType"`
	} `json:"params"`
	OrgType         string      `json:"org_type"`
	CorrelationID   interface{} `json:"correlation_id"`
	PhaseInProgress string      `json:"phase_in_progress"`
	OrgID           string      `json:"org_id"`
}

// getVSphereStorageClassSpec returns Storage Class Spec with supplied storage
// class parameters.
func getVSphereStorageClassSpec(scName string, scParameters map[string]string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, scReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool) *storagev1.StorageClass {
	if bindingMode == "" {
		bindingMode = storagev1.VolumeBindingImmediate
	}
	var sc = &storagev1.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "sc-",
		},
		Provisioner:          e2evSphereCSIDriverName,
		VolumeBindingMode:    &bindingMode,
		AllowVolumeExpansion: &allowVolumeExpansion,
	}
	// If scName is specified, use that name, else auto-generate storage class
	// name.

	if scName != "" {
		if supervisorCluster {
			sc.ObjectMeta.Name = scName
		} else {
			sc.ObjectMeta = metav1.ObjectMeta{
				Name: scName,
			}
		}
	}

	if scParameters != nil {
		sc.Parameters = scParameters
	}
	if allowedTopologies != nil {
		sc.AllowedTopologies = []v1.TopologySelectorTerm{
			{
				MatchLabelExpressions: allowedTopologies,
			},
		}
	}
	if scReclaimPolicy != "" {
		sc.ReclaimPolicy = &scReclaimPolicy
	}

	return sc
}

// getPvFromClaim returns PersistentVolume for requested claim.
func getPvFromClaim(client clientset.Interface, namespace string, claimName string) *v1.PersistentVolume {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pv
}

// getNodeUUID returns Node VM UUID for requested node.
func getNodeUUID(ctx context.Context, client clientset.Interface, nodeName string) string {
	vmUUID := ""
	if isCsiFssEnabled(ctx, client, GetAndExpectStringEnvVar(envCSINamespace), useCsiNodeID) {
		csiNode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiDriverFound := false
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name == e2evSphereCSIDriverName {
				csiDriverFound = true
				vmUUID = driver.NodeID
			}
		}
		gomega.Expect(csiDriverFound).To(gomega.BeTrue(), "CSI driver not found in CSI node %s", nodeName)
	} else {
		node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmUUID = strings.TrimPrefix(node.Spec.ProviderID, providerPrefix)
		gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
	}
	ginkgo.By(fmt.Sprintf("VM UUID is: %s for node: %s", vmUUID, nodeName))
	return vmUUID
}

// getVMUUIDFromNodeName returns the vmUUID for a given node vm and datacenter.
func getVMUUIDFromNodeName(nodeName string) (string, error) {
	var datacenters []string
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dcList := strings.Split(cfg.Global.Datacenters, ",")
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

// verifyVolumeMetadataInCNS verifies container volume metadata is matching the
// one is CNS cache.
func verifyVolumeMetadataInCNS(vs *vSphere, volumeID string,
	PersistentVolumeClaimName string, PersistentVolumeName string,
	PodName string, Labels ...vim25types.KeyValue) error {
	queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
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

// getVirtualDeviceByDiskID gets the virtual device by diskID.
func getVirtualDeviceByDiskID(ctx context.Context, vm *object.VirtualMachine,
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

// getPersistentVolumeClaimSpecWithStorageClass return the PersistentVolumeClaim
// spec with specified storage class.
func getPersistentVolumeClaimSpecWithStorageClass(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	disksize := diskSize
	if ds != "" {
		disksize = ds
	}
	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(disksize),
				},
			},
			StorageClassName: &(storageclass.Name),
		},
	}

	if pvclaimlabels != nil {
		claim.Labels = pvclaimlabels
	}

	return claim
}

// getPersistentVolumeClaimSpecWithoutStorageClass return the PersistentVolumeClaim
// spec without passing the storage class.
func getPersistentVolumeClaimSpecWithoutStorageClass(namespace string, ds string,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	disksize := diskSize
	if ds != "" {
		disksize = ds
	}
	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(disksize),
				},
			},
		},
	}

	if pvclaimlabels != nil {
		claim.Labels = pvclaimlabels
	}

	return claim
}

// createPVCAndStorageClass helps creates a storage class with specified name,
// storageclass parameters and PVC using storage class.
func createPVCAndStorageClass(client clientset.Interface, pvcnamespace string,
	pvclaimlabels map[string]string, scParameters map[string]string, ds string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode,
	names ...string) (*storagev1.StorageClass, *v1.PersistentVolumeClaim, error) {
	scName := ""
	if len(names) > 0 {
		scName = names[0]
	}
	storageclass, err := createStorageClass(client, scParameters,
		allowedTopologies, "", bindingMode, allowVolumeExpansion, scName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvclaim, err := createPVC(client, pvcnamespace, pvclaimlabels, ds, storageclass, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return storageclass, pvclaim, err
}

// createStorageClass helps creates a storage class with specified name,
// storageclass parameters.
func createStorageClass(client clientset.Interface, scParameters map[string]string,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	scReclaimPolicy v1.PersistentVolumeReclaimPolicy, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool, scName string) (*storagev1.StorageClass, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var err error
	isStorageClassPresent := false
	ginkgo.By(fmt.Sprintf("Creating StorageClass %s with scParameters: %+v and allowedTopologies: %+v "+
		"and ReclaimPolicy: %+v and allowVolumeExpansion: %t",
		scName, scParameters, allowedTopologies, scReclaimPolicy, allowVolumeExpansion))

	if supervisorCluster {
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if storageclass != nil && err == nil {
			isStorageClassPresent = true
		}
	}

	if !isStorageClassPresent {
		storageclass, err = client.StorageV1().StorageClasses().Create(ctx, getVSphereStorageClassSpec(scName,
			scParameters, allowedTopologies, scReclaimPolicy, bindingMode, allowVolumeExpansion), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
	}

	return storageclass, err
}

// updateSCtoDefault updates the given SC to default
func updateSCtoDefault(ctx context.Context, client clientset.Interface, scName, isDefault string) error {

	sc, err := client.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
	if err != nil {
		framework.Failf("Failed to get storage class %s from cluster, err=%+v", sc.Name, err)
		return err
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				"storageclass.kubernetes.io/is-default-class": isDefault,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		framework.Failf("Failed to marshal patch(%s): %s", patch, err)
		return err
	}

	// Patch the storage class with the updated annotation.
	updatedSC, err := client.StorageV1().StorageClasses().Patch(ctx,
		scName, "application/merge-patch+json", patchBytes, metav1.PatchOptions{})
	if err != nil {
		framework.Failf("Failed to patch the storage class object with isDefault. Err = %+v", err)
		return err
	}
	framework.Logf("Successfully updated annotations of storage class '%s' to:\n%v", scName, updatedSC.Annotations)
	return nil
}

// createPVC helps creates pvc with given namespace and labels using given
// storage class.
func createPVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string, ds string,
	storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode) (*v1.PersistentVolumeClaim, error) {
	pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
		storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := fpv.CreatePVC(client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
	framework.Logf("PVC created: %v in namespace: %v", pvclaim.Name, pvcnamespace)
	return pvclaim, err
}

// createPVC helps creates pvc with given namespace and labels using given
// storage class.
func scaleCreatePVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string, ds string,
	storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode, wg *sync.WaitGroup) {
	defer wg.Done()

	pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
		storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := fpv.CreatePVC(client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
	pvclaims = append(pvclaims, pvclaim)

}

// scaleCreateDeletePVC helps create and delete pvc with given namespace and
// labels, using given storage class (envWorkerPerRoutine * envNumberOfGoRoutines)
// times. Create and Delete PVC happens synchronously. PVC is picked randomly
// for deletion.
func scaleCreateDeletePVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string,
	ds string, storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode,
	wg *sync.WaitGroup, lock *sync.Mutex, worker int) {
	ctx, cancel := context.WithCancel(context.Background())
	var totalPVCDeleted int = 0
	defer cancel()
	defer wg.Done()
	for index := 1; index <= worker; index++ {
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
		ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
			storageclass.Name, ds, pvclaimlabels, accessMode))
		pvclaim, err := fpv.CreatePVC(client, pvcnamespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		lock.Lock()
		pvclaims = append(pvclaims, pvclaim)
		pvclaimToDelete := randomPickPVC()
		lock.Unlock()

		// Check if volume is present or not.
		pvclaimToDelete, err = client.CoreV1().PersistentVolumeClaims(pvclaimToDelete.Namespace).Get(
			ctx, pvclaimToDelete.Name, metav1.GetOptions{})

		if err == nil {
			// Waiting for PVC to be bound.
			pvclaimsToDelete := append(pvclaimsToDelete, pvclaimToDelete)
			_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaimsToDelete, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(client, pvclaimToDelete.Name, pvcnamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			totalPVCDeleted++
		}
	}
	framework.Logf("Total number of deleted PVCs %v", totalPVCDeleted)
}

// randomPickPVC returns randomly picked PVC from list of PVCs.
func randomPickPVC() *v1.PersistentVolumeClaim {
	index := rand.Intn(len(pvclaims))
	pvclaims[len(pvclaims)-1], pvclaims[index] = pvclaims[index], pvclaims[len(pvclaims)-1]
	pvclaimToDelete := pvclaims[len(pvclaims)-1]
	pvclaims = pvclaims[:len(pvclaims)-1]
	framework.Logf("pvc to delete %v", pvclaimToDelete.Name)
	return pvclaimToDelete
}

// createStatefulSetWithOneReplica helps create a stateful set with one replica.
func createStatefulSetWithOneReplica(client clientset.Interface, manifestPath string,
	namespace string) (*appsv1.StatefulSet, *v1.Service) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mkpath := func(file string) string {
		return filepath.Join(manifestPath, file)
	}
	statefulSet, err := manifest.StatefulSetFromManifest(mkpath("statefulset.yaml"), namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	service, err := manifest.SvcFromManifest(mkpath("service.yaml"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	service, err = client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*statefulSet.Spec.Replicas = 1
	_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulSet, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return statefulSet, service
}

// updateDeploymentReplicawithWait helps to update the replica for a deployment
// with wait.
func updateDeploymentReplicawithWait(client clientset.Interface, count int32, name string, namespace string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deployment *appsv1.Deployment
	var err error
	waitErr := wait.Poll(healthStatusPollInterval, healthStatusPollTimeout, func() (bool, error) {
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if err != nil {
			return false, nil
		}
		*deployment.Spec.Replicas = count
		ginkgo.By("Waiting for update operation on deployment to take effect")
		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			return false, err
		}
		err = fdep.WaitForDeploymentComplete(client, deployment)
		if err != nil {
			return false, err
		}
		return true, nil
	})
	return waitErr
}

// updateDeploymentReplica helps to update the replica for a deployment.
func updateDeploymentReplica(client clientset.Interface,
	count int32, name string, namespace string) *appsv1.Deployment {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*deployment.Spec.Replicas = count
	deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = fdep.WaitForDeploymentComplete(client, deployment)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return deployment
}

// bringDownCsiController helps to bring the csi controller pod down.
// Default namespace used here is csiSystemNamespace.
func bringDownCsiController(Client clientset.Interface, namespace ...string) {
	if len(namespace) == 0 {
		updateDeploymentReplica(Client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
	} else {
		updateDeploymentReplica(Client, 0, vSphereCSIControllerPodNamePrefix, namespace[0])
	}
	ginkgo.By("Controller is down")
}

// bringDownTKGController helps to bring the TKG control manager pod down.
// Its taks svc client as input.
func bringDownTKGController(Client clientset.Interface) {
	updateDeploymentReplica(Client, 0, vsphereControllerManager, vsphereTKGSystemNamespace)
	ginkgo.By("TKGControllManager replica is set to 0")
}

// bringUpCsiController helps to bring the csi controller pod down.
// Default namespace used here is csiSystemNamespace.
func bringUpCsiController(Client clientset.Interface, csiReplicaCount int32, namespace ...string) {
	if len(namespace) == 0 {
		err := updateDeploymentReplicawithWait(Client, csiReplicaCount,
			vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		err := updateDeploymentReplicawithWait(Client, csiReplicaCount,
			vSphereCSIControllerPodNamePrefix, namespace[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	ginkgo.By("Controller is up")
}

// bringUpTKGController helps to bring the TKG control manager pod up.
// Its taks svc client as input.
func bringUpTKGController(Client clientset.Interface, tkgReplica int32) {
	err := updateDeploymentReplicawithWait(Client, tkgReplica, vsphereControllerManager, vsphereTKGSystemNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("TKGControllManager is up")
}

func getSvcClientAndNamespace() (clientset.Interface, string) {
	var err error
	if svcClient == nil {
		if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
			svcClient, err = createKubernetesClientFromConfig(k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		svcNamespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	}
	return svcClient, svcNamespace
}

// updateCSIDeploymentTemplateFullSyncInterval helps to update the
// FULL_SYNC_INTERVAL_MINUTES in deployment template. For this to take effect,
// we need to terminate the running csi controller pod.
// Returns fsync interval value before the change.
func updateCSIDeploymentTemplateFullSyncInterval(client clientset.Interface, mins string, namespace string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deployment, err := client.AppsV1().Deployments(namespace).Get(
		ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	containers := deployment.Spec.Template.Spec.Containers

	for _, c := range containers {
		if c.Name == "vsphere-syncer" {
			for _, e := range c.Env {
				if e.Name == "FULL_SYNC_INTERVAL_MINUTES" {
					e.Value = mins
				}
			}
		}
	}
	deployment.Spec.Template.Spec.Containers = containers
	_, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Polling for update operation on deployment to take effect...")
	waitErr := wait.Poll(healthStatusPollInterval, healthStatusPollTimeout, func() (bool, error) {
		deployment, err = client.AppsV1().Deployments(namespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if err != nil {
			return false, nil
		}
		err = fdep.WaitForDeploymentComplete(client, deployment)
		if err != nil {
			return false, err
		}
		return true, nil
	})
	return waitErr
}

// updateCSIDeploymentProvisionerTimeout helps to update the timeout value
// in deployment template. For this to take effect,
func updateCSIDeploymentProvisionerTimeout(client clientset.Interface, namespace string, timeout string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ignoreLabels := make(map[string]string)
	list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	num_csi_pods := len(list_of_pods)

	deployment, err := client.AppsV1().Deployments(namespace).Get(
		ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	containers := deployment.Spec.Template.Spec.Containers
	for _, c := range containers {
		if c.Name == "csi-provisioner" {
			for i, arg := range c.Args {
				if strings.Contains(arg, "timeout") {
					if strings.Contains(arg, "--timeout="+timeout+"s") {
						framework.Logf("Provisioner Timeout value is already set to expected value, hence return")
						return
					} else {
						c.Args[i] = "--timeout=" + timeout + "s"
					}
				}
			}
		}
	}
	deployment.Spec.Template.Spec.Containers = containers
	_, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Waiting for a min for update operation on deployment to take effect...")
	time.Sleep(1 * time.Minute)
	err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
		pollTimeout, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// getLabelsMapFromKeyValue returns map[string]string for given array of
// vim25types.KeyValue.
func getLabelsMapFromKeyValue(labels []vim25types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// getDatastoreByURL returns the *Datastore instance given its URL.
func getDatastoreByURL(ctx context.Context, datastoreURL string, dc *object.Datacenter) (*object.Datastore, error) {
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

// getPersistentVolumeClaimSpec gets vsphere persistent volume spec with given
// selector labels and binds it to given pv.
func getPersistentVolumeClaimSpec(namespace string, labels map[string]string, pvName string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// Create PV volume spec with given FCD ID, Reclaim Policy and labels.
func getPersistentVolumeSpec(fcdID string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	labels map[string]string, fstype string) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: fcdID,
				ReadOnly:     false,
				FSType:       fstype,
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:         claimRef,
			StorageClassName: "",
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// Create PVC spec with given namespace, labels and pvName.
func getPersistentVolumeClaimSpecForRWX(namespace string, labels map[string]string,
	pvName string, pvSize string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	if pvSize == "" {
		pvSize = "2Gi"
	}
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(pvSize),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// Create PV volume spec with given FCD ID, Reclaim Policy and labels.
func getPersistentVolumeSpecForRWX(fcdID string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	labels map[string]string, pvSize string, storageclass string,
	accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)

	if pvSize == "" {
		pvSize = "2Gi"
	}

	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteMany
	}

	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: fcdID,
				ReadOnly:     false,
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(pvSize),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			ClaimRef:         claimRef,
			StorageClassName: storageclass,
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// invokeVCenterReboot invokes reboot command on the given vCenter over SSH.
func invokeVCenterReboot(host string) error {
	sshCmd := "reboot"
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// invokeVCenterServiceControl invokes the given command for the given service
// via service-control on the given vCenter host over SSH.
func invokeVCenterServiceControl(command, service, host string) error {
	sshCmd := fmt.Sprintf("service-control --%s %s", command, service)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", sshCmd, host, err)
	}
	return nil
}

/*
	Note: As per PR #2935677, even if cns_new_sync is enabled volume expansion
	will not work if sps-service is down.
	Keeping this code for reference. Disabling isFssEnabled util method as we won't be using
	this util method in testcases.
	isFssEnabled invokes the given command to check if vCenter has a particular FSS enabled or not
*/
// func isFssEnabled(host, fss string) bool {
// 	sshCmd := fmt.Sprintf("python /usr/sbin/feature-state-wrapper.py %s", fss)
// 	framework.Logf("Checking if fss is enabled on vCenter host %v", host)
// 	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
// 	fssh.LogResult(result)
// 	if err == nil && result.Code == 0 {
// 		return strings.TrimSpace(result.Stdout) == "enabled"
// 	} else {
// 		ginkgo.By(fmt.Sprintf("couldn't execute command: %s on vCenter host: %v", sshCmd, err))
// 		gomega.Expect(err).NotTo(gomega.HaveOccurred())
// 	}
// 	return false
// }

// waitVCenterServiceToBeInState invokes the status check for the given service and waits
// via service-control on the given vCenter host over SSH.
func waitVCenterServiceToBeInState(serviceName string, host string, state string) error {
	waitErr := wait.PollImmediate(poll, pollTimeoutShort*2, func() (bool, error) {
		sshCmd := fmt.Sprintf("service-control --%s %s", "status", serviceName)
		framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
		result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)

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

// checkVcenterServicesStatus checks and polls for vCenter essential services status
// to be in running state
func checkVcenterServicesRunning(
	ctx context.Context, host string, essentialServices []string, timeout ...time.Duration) {

	var pollTime time.Duration
	if len(timeout) == 0 {
		pollTime = pollTimeout * 2
	} else {
		pollTime = timeout[0]
	}
	waitErr := wait.PollImmediate(poll, pollTime, func() (bool, error) {
		var runningServices []string
		var statusMap = make(map[string]bool)
		allServicesRunning := true
		sshCmd := fmt.Sprintf("service-control --%s", statusOperation)
		framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
		result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
		if err != nil || result.Code != 0 {
			fssh.LogResult(result)
			return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
		}
		framework.Logf("Command %v output is %v", sshCmd, result.Stdout)

		services := strings.SplitAfter(result.Stdout, svcRunningMessage+":")
		stoppedService := strings.Split(services[1], svcStoppedMessage+":")
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
}

// httpRequest takes client and http Request as input and performs GET operation
// and returns bodybytes
func httpRequest(client *http.Client, req *http.Request) ([]byte, int) {
	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("API Response status %d", resp.StatusCode)

	return bodyBytes, resp.StatusCode

}

// getVMImages returns the available gc images present in svc
func getVMImages(wcpHost string, wcpToken string) VMImages {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	var vmImage VMImages
	client := &http.Client{Transport: transCfg}
	getVirtualMachineImagesURL := "https://" + wcpHost + vmOperatorAPI + "virtualmachineimages"
	framework.Logf("URL %v", getVirtualMachineImagesURL)
	wcpToken = "Bearer " + wcpToken
	req, err := http.NewRequest("GET", getVirtualMachineImagesURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	bodyBytes, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	err = json.Unmarshal(bodyBytes, &vmImage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return vmImage
}

// deleteTKG method deletes the TKG Cluster
func deleteTKG(wcpHost string, wcpToken string, tkgCluster string) error {
	ginkgo.By("Delete TKG")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + tkgAPI + tkgCluster
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken

	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	_, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	req, err = http.NewRequest("DELETE", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	_, statusCode = httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	//get gc and validate if gc is deleted
	req, err = http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)

	err = waitForDeleteToComplete(client, req)

	return err

}

// waitForDeleteToComplete method polls for the requested object status
// returns true if its deleted successfully else returns error
func waitForDeleteToComplete(client *http.Client, req *http.Request) error {
	waitErr := wait.Poll(pollTimeoutShort, pollTimeout*6, func() (bool, error) {
		framework.Logf("Polling for New GC status")
		_, statusCode := httpRequest(client, req)

		if statusCode == 404 {
			framework.Logf("requested object is deleted")
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// upgradeTKG method updates the TKG Cluster with the tkgImage
func upgradeTKG(wcpHost string, wcpToken string, tkgCluster string, tkgImage string) {
	ginkgo.By("Upgrade TKG")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + tkgAPI + tkgCluster
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken

	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	bodyBytes, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var tkg TKGCluster
	err = yaml.Unmarshal(bodyBytes, &tkg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("tkg versions %s", tkg.Spec.Distribution.Version)
	tkg.Spec.Distribution.FullVersion = ""
	tkg.Spec.Distribution.Version = tkgImage
	framework.Logf("tkg cluster %v", tkg)

	new_data, err := yaml.Marshal(&tkg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req, err = http.NewRequest("PUT", getGCURL, bytes.NewBuffer(new_data))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	req.Header.Add("Accept", "application/yaml")
	req.Header.Add("Content-Type", "application/yaml")

	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()

	bodyBytes, err = io.ReadAll(resp.Body)
	framework.Logf("API Response status %v", resp.StatusCode)
	gomega.Expect(resp.StatusCode).Should(gomega.BeNumerically("==", 200))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	response := string(bodyBytes)
	framework.Logf("response %v", response)

}

// createGC method creates GC and takes WCP host and bearer token as input param
func createGC(wcpHost string, wcpToken string) {

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	createGCURL := "https://" + wcpHost + tkgAPI
	framework.Logf("URL %v", createGCURL)
	tkg_yaml, err := filepath.Abs(gcManifestPath + "tkg.yaml")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Taking yaml from %v", tkg_yaml)
	gcBytes, err := os.ReadFile(tkg_yaml)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req, err := http.NewRequest("POST", createGCURL, bytes.NewBuffer(gcBytes))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", "Bearer "+wcpToken)
	req.Header.Add("Accept", "application/yaml")
	req.Header.Add("Content-Type", "application/yaml")
	bodyBytes, statusCode := httpRequest(client, req)

	response := string(bodyBytes)
	framework.Logf(response)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 201))
}

func getAuthToken(refreshToken string) string {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	data := url.Values{}
	data.Set("refresh_token", refreshToken)

	client := &http.Client{Transport: transCfg}
	req, err := http.NewRequest("POST", authAPI, strings.NewReader(data.Encode()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer resp.Body.Close()
	gomega.Expect(resp.StatusCode).Should(gomega.BeNumerically("==", 200))

	auth := AuthToken{}
	err = json.NewDecoder(resp.Body).Decode(&auth)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return auth.AccessToken

}

// rotateVCCertinVMC recreates the cert for VC in VMC environment
func rotateVCCertinVMC(authToken string, orgId string, sddcId string) string {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	client := &http.Client{Transport: transCfg}
	certRotateURL := vmcPrdEndpoint + orgId + "/sddcs/" + sddcId + "/certificate/VCENTER"

	req, err := http.NewRequest("POST", certRotateURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req.Header.Add("csp-auth-token", authToken)
	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()

	certRotate := CertRotate{}
	err = json.NewDecoder(resp.Body).Decode(&certRotate)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.StatusCode).Should(gomega.BeNumerically("==", 202))
	framework.Logf("task ID %s ", certRotate.ID)

	return certRotate.ID
}

// getTaskStatus polls status for given task id and returns true once task is completed
func getTaskStatus(authToken string, orgID string, taskID string) error {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	client := &http.Client{Transport: transCfg}
	getTaskStatusURL := vmcPrdEndpoint + orgID + "/tasks/" + taskID
	framework.Logf("getTaskStatusURL %s", getTaskStatusURL)

	req, err := http.NewRequest("GET", getTaskStatusURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	waitErr := wait.Poll(pollTimeoutShort, pollTimeoutShort*5, func() (bool, error) {
		framework.Logf("Polling for Task Status")
		req.Header.Add("csp-auth-token", authToken)
		resp, err := client.Do(req)
		gomega.Expect(resp.StatusCode).Should(gomega.BeNumerically("==", 200))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer resp.Body.Close()

		getTaskStatus := GetTaskTstatus{}
		err = json.NewDecoder(resp.Body).Decode(&getTaskStatus)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if getTaskStatus.ProgressPercent == 100 && getTaskStatus.Status == "FINISHED" {
			framework.Logf("VC Cert Rotate Task Finished")
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// scaleTKGWorker scales the TKG worker nodes on given tkgCluster based on the tkgworker count
func scaleTKGWorker(wcpHost string, wcpToken string, tkgCluster string, tkgworker int) {

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + tkgAPI + tkgCluster
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken
	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	bodyBytes, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var tkg TKGCluster
	err = yaml.Unmarshal(bodyBytes, &tkg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("tkg version %s", tkg.Spec.Distribution.Version)
	tkg.Spec.Topology.Workers.Count = tkgworker

	new_data, err := yaml.Marshal(&tkg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req, err = http.NewRequest("PUT", getGCURL, bytes.NewBuffer(new_data))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	req.Header.Add("Accept", "application/yaml")
	req.Header.Add("Content-Type", "application/yaml")
	bodyBytes, statusCode = httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	response := string(bodyBytes)
	framework.Logf("response %v", response)

}

// getGC polls for the GC status, returns error if its not in running phase
func getGC(wcpHost string, wcpToken string, gcName string) error {
	var response string
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + tkgAPI + gcName
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken
	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)

	waitErr := wait.Poll(pollTimeoutShort, pollTimeout*6, func() (bool, error) {
		framework.Logf("Polling for New GC status")
		bodyBytes, statusCode := httpRequest(client, req)
		gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))
		response = string(bodyBytes)

		if strings.Contains(response, "\"phase\":\"running\"") {
			framework.Logf("new gc is up and running")
			return true, nil
		}
		return false, nil
	})
	framework.Logf(response)
	return waitErr
}

// getWCPSessionId returns the bearer token for given user
func getWCPSessionId(hostname string, username string, password string) string {
	type WcpSessionID struct {
		Session_id string
	}

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	URL := "https://" + hostname + "/wcp/login"
	framework.Logf("URL %s", URL)

	req, err := http.NewRequest("POST", URL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(username, password)
	bodyBytes, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var wcpSessionID WcpSessionID
	err = json.Unmarshal(bodyBytes, &wcpSessionID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("SessionID: %s", wcpSessionID.Session_id)

	return wcpSessionID.Session_id

}

// replacePasswordRotationTime invokes the given command to replace the password
// rotation time to 0, so that password roation happens immediately on the given
// vCenter over SSH. Vmon-cli is used to restart the wcp service after changing
// the time.
func replacePasswordRotationTime(file, host string) error {
	sshCmd := fmt.Sprintf("sed -i '3 c\\0' %s", file)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf("vmon-cli -r %s", wcpServiceName)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err = fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	time.Sleep(sleepTimeOut)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// getVCentreSessionId gets vcenter session id to work with vcenter apis
func getVCentreSessionId(hostname string, username string, password string) string {

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	URL := "https://" + hostname + "/api/session"

	req, err := http.NewRequest("POST", URL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	bodyBytes, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 201))

	var sessionID string
	err = json.Unmarshal(bodyBytes, &sessionID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("SessionID: %s", sessionID)

	return sessionID
}

// getWCPCluster get the wcp cluster details
func getWCPCluster(sessionID string, hostIP string) string {

	type WCPCluster struct {
		Cluster string
	}

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	clusterURL := "https://" + hostIP + vcClusterAPI
	framework.Logf("URL %v", clusterURL)

	req, err := http.NewRequest("GET", clusterURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req.Header.Add("vmware-api-session-id", sessionID)

	bodyBytes, statusCode := httpRequest(client, req)

	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var wcpCluster []WCPCluster
	err = json.Unmarshal(bodyBytes, &wcpCluster)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("wcp cluster %+v\n", wcpCluster[0].Cluster)

	return wcpCluster[0].Cluster
}

// getWCPHost gets the wcp host details
func getWCPHost(wcpCluster string, hostIP string, sessionID string) string {
	type WCPClusterInfo struct {
		Api_server_management_endpoint string `json:"api_server_management_endpoint"`
	}
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	client := &http.Client{Transport: transCfg}
	clusterURL := "https://" + hostIP + vcClusterAPI + "/" + wcpCluster
	framework.Logf("URL %v", clusterURL)

	req, err := http.NewRequest("GET", clusterURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("vmware-api-session-id", sessionID)
	bodyBytes, statusCode := httpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var wcpClusterInfo WCPClusterInfo
	err = json.Unmarshal(bodyBytes, &wcpClusterInfo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("wcp cluster IP %+v\n", wcpClusterInfo.Api_server_management_endpoint)

	return wcpClusterInfo.Api_server_management_endpoint
}

// writeToFile will take two parameters:
// 1. the absolute path of the file(including filename) to be created.
// 2. data content to be written into the file.
// Returns nil on Success and error on failure.
func writeToFile(filePath, data string) error {
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

// invokeVCenterChangePassword invokes `dir-cli password reset` command on the
// given vCenter host over SSH, thereby resetting the currentPassword of the
// `user` to the `newPassword`.
func invokeVCenterChangePassword(user, adminPassword, newPassword, host string) error {
	// Create an input file and write passwords into it.
	path := "input.txt"
	data := fmt.Sprintf("%s\n%s\n", adminPassword, newPassword)
	err := writeToFile(path, data)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		// Delete the input file containing passwords.
		err = os.Remove(path)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	// Remote copy this input file to VC.
	copyCmd := fmt.Sprintf("/bin/cat %s | /usr/bin/ssh root@%s '/usr/bin/cat >> input_copy.txt'",
		path, e2eVSphere.Config.Global.VCenterHostname)
	fmt.Printf("Executing the command: %s\n", copyCmd)
	_, err = exec.Command("/bin/sh", "-c", copyCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		// Remove the input_copy.txt file from VC.
		removeCmd := fmt.Sprintf("/usr/bin/ssh root@%s '/usr/bin/rm input_copy.txt'",
			e2eVSphere.Config.Global.VCenterHostname)
		_, err = exec.Command("/bin/sh", "-c", removeCmd).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	sshCmd :=
		fmt.Sprintf("/usr/bin/cat input_copy.txt | /usr/lib/vmware-vmafd/bin/dir-cli password reset --account %s", user)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v, err: %v", sshCmd, host, err)
	}
	if !strings.Contains(result.Stdout, "Password was reset successfully for ") {
		framework.Logf("failed to change the password for user %s: %s", user, result.Stdout)
		return err
	}
	framework.Logf("password changed successfully for user: %s", user)
	return nil
}

// verifyVolumeTopology verifies that the Node Affinity rules in the volume.
// Match the topology constraints specified in the storage class.
func verifyVolumeTopology(pv *v1.PersistentVolume, zoneValues []string, regionValues []string) (string, string, error) {
	if pv.Spec.NodeAffinity == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return "", "", fmt.Errorf("node Affinity rules for PV should exist in topology aware provisioning")
	}
	var pvZone string
	var pvRegion string
	for _, labels := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions {
		if labels.Key == zoneKey {
			for _, value := range labels.Values {
				gomega.Expect(zoneValues).To(gomega.ContainElement(value),
					fmt.Sprintf("Node Affinity rules for PV %s: %v does not contain zone specified in storage class %v",
						pv.Name, value, zoneValues))
				pvZone = value
			}
		}
		if labels.Key == regionKey {
			for _, value := range labels.Values {
				gomega.Expect(regionValues).To(gomega.ContainElement(value),
					fmt.Sprintf("Node Affinity rules for PV %s: %v does not contain region specified in storage class %v",
						pv.Name, value, regionValues))
				pvRegion = value
			}
		}
	}
	framework.Logf("PV %s is located in zone: %s and region: %s", pv.Name, pvZone, pvRegion)
	return pvRegion, pvZone, nil
}

// verifyPodLocation verifies that a pod is scheduled on a node that belongs
// to the topology on which PV is provisioned.
func verifyPodLocation(pod *v1.Pod, nodeList *v1.NodeList, zoneValue string, regionValue string) error {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			for labelKey, labelValue := range node.Labels {
				if labelKey == zoneKey && zoneValue != "" {
					gomega.Expect(zoneValue).To(gomega.Equal(labelValue),
						fmt.Sprintf("Pod %s is not running on Node located in zone %v", pod.Name, zoneValue))
				}
				if labelKey == regionKey && regionValue != "" {
					gomega.Expect(regionValue).To(gomega.Equal(labelValue),
						fmt.Sprintf("Pod %s is not running on Node located in region %v", pod.Name, regionValue))
				}
			}
		}
	}
	return nil
}

// getTopologyFromPod rturn topology value from node affinity information.
func getTopologyFromPod(pod *v1.Pod, nodeList *v1.NodeList) (string, string, error) {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			podRegion := node.Labels[v1.LabelZoneRegion]
			podZone := node.Labels[v1.LabelZoneFailureDomain]
			return podRegion, podZone, nil
		}
	}
	err := errors.New("could not find the topology from pod")
	return "", "", err
}

// topologyParameterForStorageClass creates a topology map using the topology
// values ENV variables. Returns the allowedTopologies parameters required for
// the Storage Class.
// Input : <region-1>:<zone-1>, <region-1>:<zone-2>
// Output : [region-1], [zone-1, zone-2] {region-1: zone-1, region-1:zone-2}
func topologyParameterForStorageClass(topology string) ([]string, []string, []v1.TopologySelectorLabelRequirement) {
	topologyMap := createTopologyMap(topology)
	regionValues, zoneValues := getValidTopology(topologyMap)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{
		{
			Key:    regionKey,
			Values: regionValues,
		},
		{
			Key:    zoneKey,
			Values: zoneValues,
		},
	}
	return regionValues, zoneValues, allowedTopologies
}

// createTopologyMap strips the topology string provided in the environment
// variable into a map from region to zones.
// example envTopology = "r1:z1,r1:z2,r2:z3"
func createTopologyMap(topologyString string) map[string][]string {
	topologyMap := make(map[string][]string)
	for _, t := range strings.Split(topologyString, ",") {
		t = strings.TrimSpace(t)
		topology := strings.Split(t, ":")
		if len(topology) != 2 {
			continue
		}
		topologyMap[topology[0]] = append(topologyMap[topology[0]], topology[1])
	}
	return topologyMap
}

// getValidTopology returns the regions and zones from the input topology map
// so that they can be provided to a storage class.
func getValidTopology(topologyMap map[string][]string) ([]string, []string) {
	var regionValues []string
	var zoneValues []string
	for region, zones := range topologyMap {
		regionValues = append(regionValues, region)
		zoneValues = append(zoneValues, zones...)
	}
	return regionValues, zoneValues
}

// createResourceQuota creates resource quota for the specified namespace.
func createResourceQuota(client clientset.Interface, namespace string, size string, scName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitTime := 15
	var executeCreateResourceQuota bool
	executeCreateResourceQuota = true
	storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

	if supervisorCluster {
		_, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, namespace+"-storagequota", metav1.GetOptions{})
		if err != nil || !(scName == storagePolicyNameForSharedDatastores) {
			executeCreateResourceQuota = true
		} else {
			executeCreateResourceQuota = false
		}
	}

	if executeCreateResourceQuota {
		// deleteResourceQuota if already present.
		deleteResourceQuota(client, namespace)

		resourceQuota := newTestResourceQuota(quotaName, size, scName)
		resourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Create(ctx, resourceQuota, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Create Resource quota: %+v", resourceQuota))
		ginkgo.By(fmt.Sprintf("Waiting for %v seconds to allow resourceQuota to be claimed", waitTime))
		time.Sleep(time.Duration(waitTime) * time.Second)
	}
}

// deleteResourceQuota deletes resource quota for the specified namespace,
// if it exists.
func deleteResourceQuota(client clientset.Interface, namespace string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, quotaName, metav1.GetOptions{})
	if err == nil {
		err = client.CoreV1().ResourceQuotas(namespace).Delete(ctx, quotaName, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Deleted Resource quota: %+v", quotaName))
	}
}

// checks if resource quota gets updated or not
func checkResourceQuota(client clientset.Interface, namespace string, name string, size string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waitErr := wait.PollImmediate(poll, pollTimeoutShort, func() (bool, error) {
		currentResourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("currentResourceQuota %v", currentResourceQuota)
		if err != nil {
			return false, err
		}
		framework.Logf("current size of quota %v", currentResourceQuota.Status.Used["requests.storage"])
		if currentResourceQuota.Status.Hard[v1.ResourceName("requests.storage")] == resource.MustParse(size) {
			ginkgo.By("Resource quota updated")
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// setResourceQuota resource quota to the specified limit for the given namespace.
func setResourceQuota(client clientset.Interface, namespace string, size string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// deleteResourceQuota if already present.
	deleteResourceQuota(client, namespace)
	existingResourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, namespace, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("existingResourceQuota name %s and requested size %v", existingResourceQuota.GetName(), size)
		requestStorageQuota := updatedSpec4ExistingResourceQuota(existingResourceQuota.GetName(), size)
		testResourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Update(
			ctx, requestStorageQuota, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("ResourceQuota details: %+v", testResourceQuota))
		err = checkResourceQuota(client, namespace, existingResourceQuota.GetName(), size)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// newTestResourceQuota returns a quota that enforces default constraints for
// testing.
func newTestResourceQuota(name string, size string, scName string) *v1.ResourceQuota {
	hard := v1.ResourceList{}
	// Test quota on discovered resource type.
	hard[v1.ResourceName(scName+rqStorageType)] = resource.MustParse(size)
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: hard},
	}
}

// updatedSpec4ExistingResourceQuota returns a quota that enforces default
// constraints for testing.
func updatedSpec4ExistingResourceQuota(name string, size string) *v1.ResourceQuota {
	updateQuota := v1.ResourceList{}
	// Test quota on discovered resource type.
	updateQuota[v1.ResourceName("requests.storage")] = resource.MustParse(size)
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: updateQuota},
	}
}

// checkEventsforError prints the list of all events that occurred in the
// namespace and searches for expectedErrorMsg among these events.
func checkEventsforError(client clientset.Interface, namespace string,
	listOptions metav1.ListOptions, expectedErrorMsg string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventList, _ := client.CoreV1().Events(namespace).List(ctx, listOptions)
	isFailureFound := false
	for _, item := range eventList.Items {
		framework.Logf("EventList item: %q", item.Message)
		if strings.Contains(item.Message, expectedErrorMsg) {
			isFailureFound = true
			break
		}
	}
	return isFailureFound
}

// getNamespaceToRunTests returns the namespace in which the tests are expected
// to run. For Vanilla & GuestCluster test setups, returns random namespace name
// generated by the framework. For SupervisorCluster test setup, returns the
// user created namespace where pod vms will be provisioned.
func getNamespaceToRunTests(f *framework.Framework) string {
	if supervisorCluster {
		return GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	}
	return f.Namespace.Name
}

// getPVCFromSupervisorCluster takes name of the persistentVolumeClaim as input,
// returns the corresponding persistentVolumeClaim object.
func getPVCFromSupervisorCluster(pvcName string) *v1.PersistentVolumeClaim {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svcClient, svcNamespace := getSvcClientAndNamespace()
	pvc, err := svcClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvc).NotTo(gomega.BeNil())
	return pvc
}

// getVolumeIDFromSupervisorCluster returns SV PV volume handle for given SVC
// PVC name.
func getVolumeIDFromSupervisorCluster(pvcName string) string {
	var svcClient clientset.Interface
	var err error
	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = createKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	svcPV := getPvFromClaim(svcClient, svNamespace, pvcName)
	volumeHandle := svcPV.Spec.CSI.VolumeHandle
	ginkgo.By(fmt.Sprintf("Found volume in Supervisor cluster with VolumeID: %s", volumeHandle))
	return volumeHandle
}

// getPvFromSupervisorCluster returns SV PV for given SVC PVC name.
func getPvFromSupervisorCluster(pvcName string) *v1.PersistentVolume {
	var svcClient clientset.Interface
	var err error
	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = createKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	svcPV := getPvFromClaim(svcClient, svNamespace, pvcName)
	return svcPV
}

func verifyFilesExistOnVSphereVolume(namespace string, podName string, filePaths ...string) {
	for _, filePath := range filePaths {
		_, err := framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
			podName, "--", "/bin/ls", filePath)
		framework.ExpectNoError(err, fmt.Sprintf("failed to verify file: %q on the pod: %q", filePath, podName))
	}
}

func createEmptyFilesOnVSphereVolume(namespace string, podName string, filePaths []string) {
	for _, filePath := range filePaths {
		err := framework.CreateEmptyFileOnPod(namespace, podName, filePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// CreateService creates a k8s service as described in the service.yaml present
// in the manifest path and returns that service to the caller.
func CreateService(ns string, c clientset.Interface) *v1.Service {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svcManifestFilePath := filepath.Join(manifestPath, "service.yaml")
	framework.Logf("Parsing service from %v", svcManifestFilePath)
	svc, err := manifest.SvcFromManifest(svcManifestFilePath)
	framework.ExpectNoError(err)

	service, err := c.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return service
}

// deleteService deletes a k8s service.
func deleteService(ns string, c clientset.Interface, service *v1.Service) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.CoreV1().Services(ns).Delete(ctx, service.Name, *metav1.NewDeleteOptions(0))
	framework.ExpectNoError(err)
}

// GetStatefulSetFromManifest creates a StatefulSet from the statefulset.yaml
// file present in the manifest path.
func GetStatefulSetFromManifest(ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(manifestPath, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	return ss
}

// isDatastoreBelongsToDatacenterSpecifiedInConfig checks whether the given
// datastoreURL belongs to the datacenter specified in the vSphere.conf file.
func isDatastoreBelongsToDatacenterSpecifiedInConfig(datastoreURL string) bool {
	var datacenters []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

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
		defaultDatastore, err := getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		if defaultDatastore != nil && err == nil {
			return true
		}
	}

	// Loop through all datacenters specified in conf file, and cannot find this
	// given datastore.
	return false
}

func getTargetvSANFileShareDatastoreURLsFromConfig() []string {
	var targetDsURLs []string
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if cfg.Global.TargetvSANFileShareDatastoreURLs != "" {
		targetDsURLs = strings.Split(cfg.Global.TargetvSANFileShareDatastoreURLs, ",")
	}
	return targetDsURLs
}

func isDatastorePresentinTargetvSANFileShareDatastoreURLs(datastoreURL string) bool {
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	datastoreURL = strings.TrimSpace(datastoreURL)
	targetDatastoreUrls := strings.Split(cfg.Global.TargetvSANFileShareDatastoreURLs, ",")
	for _, dsURL := range targetDatastoreUrls {
		dsURL = strings.TrimSpace(dsURL)
		if datastoreURL == dsURL {
			return true
		}
	}
	return false
}

func verifyVolumeExistInSupervisorCluster(pvcName string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svcClient, svNamespace := getSvcClientAndNamespace()
	svPvc, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	framework.Logf("PVC in supervisor namespace: %s", svPvc.Name)
	return true
}

func waitTillVolumeIsDeletedInSvc(pvcName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v to check whether volume  %s is deleted", timeout, pvcName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		svcClient, svNamespace := getSvcClientAndNamespace()
		svPvc, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err == nil {
			framework.Logf("Volume %s is not removed yet", pvcName)
			continue
		}
		if err != nil {
			if apierrors.IsNotFound(err) {
				framework.Logf("volume %s was removed", pvcName)
				return nil
			}
			framework.Logf("PVC in supervisor namespace: %s", svPvc.Name)
		}
	}
	return fmt.Errorf("volume %s still exists within %v", pvcName, timeout)
}

// returns crd if found by name.
func getCnsNodeVMAttachmentByName(ctx context.Context, f *framework.Framework, expectedInstanceName string,
	crdVersion string, crdGroup string) *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: "cnsnodevmattachments"}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, crd := range list.Items {
		instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if expectedInstanceName == instance.Name {
			ginkgo.By(fmt.Sprintf("Found CNSNodeVMAttachment crd: %v, expected: %v", instance, expectedInstanceName))
			framework.Logf("instance attached  is : %t\n", instance.Status.Attached)
			return instance
		}
	}
	return nil
}

// verifyIsAttachedInSupervisor verifies the crd instance is attached in
// supervisor.
func verifyIsAttachedInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdVersion string, crdGroup string) {
	instance := getCnsNodeVMAttachmentByName(ctx, f, expectedInstanceName, crdVersion, crdGroup)
	if instance != nil {
		framework.Logf("instance attached found to be : %t\n", instance.Status.Attached)
		gomega.Expect(instance.Status.Attached).To(gomega.BeTrue())
	}
	gomega.Expect(instance).NotTo(gomega.BeNil())
}

// verifyIsDetachedInSupervisor verifies the crd instance is detached from
// supervisor.
func verifyIsDetachedInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdVersion string, crdGroup string) {
	instance := getCnsNodeVMAttachmentByName(ctx, f, expectedInstanceName, crdVersion, crdGroup)
	if instance != nil {
		framework.Logf("instance attached found to be : %t\n", instance.Status.Attached)
		gomega.Expect(instance.Status.Attached).To(gomega.BeFalse())
	}
	gomega.Expect(instance).To(gomega.BeNil())
}

// verifyPodCreation helps to create/verify and delete the pod in given
// namespace. It takes client, namespace, pvc, pv as input.
func verifyPodCreation(f *framework.Framework, client clientset.Interface, namespace string,
	pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) {
	ginkgo.By("Create pod and wait for this to be in running phase")
	pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// svcPVCName refers to PVC Name in the supervisor cluster.
	svcPVCName := pv.Spec.CSI.VolumeHandle

	ginkgo.By(fmt.Sprintf("Verify cnsnodevmattachment is created with name : %s ", pod.Spec.NodeName))
	verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
		crdCNSNodeVMAttachment, crdVersion, crdGroup, true)
	verifyIsAttachedInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName, crdVersion, crdGroup)

	ginkgo.By("Deleting the pod")
	err = fpod.DeletePodWithWait(client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	ginkgo.By("Verify CnsNodeVmAttachment CRDs are deleted")
	verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
		crdCNSNodeVMAttachment, crdVersion, crdGroup, false)

}

// verifyCRDInSupervisor is a helper method to check if a given crd is
// created/deleted in the supervisor cluster. This method will fetch the list
// of CRD Objects for a given crdName, Version and Group and then verifies
// if the given expectedInstanceName exist in the list.
func verifyCRDInSupervisorWithWait(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string, isCreated bool) {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	var instanceFound bool

	const timeout time.Duration = 30
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		list, err := resourceClient.List(ctx, metav1.ListOptions{})
		if err != nil || list == nil {
			continue
		}
		if list != nil {
			for _, crd := range list.Items {
				if crdName == "cnsnodevmattachments" {
					instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
					err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					if expectedInstanceName == instance.Name {
						ginkgo.By(fmt.Sprintf("Found CNSNodeVMAttachment crd: %v, expected: %v",
							instance, expectedInstanceName))
						instanceFound = true
						break
					}
				}
				if crdName == "cnsvolumemetadatas" {
					instance := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
					err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					if expectedInstanceName == instance.Name {
						ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v",
							instance, expectedInstanceName))
						instanceFound = true
						break
					}
				}
			}
		}
		if isCreated {
			gomega.Expect(instanceFound).To(gomega.BeTrue())
		} else {
			gomega.Expect(instanceFound).To(gomega.BeFalse())
		}
	}
}

// verifyCRDInSupervisor is a helper method to check if a given crd is
// created/deleted in the supervisor cluster. This method will fetch the list
// of CRD Objects for a given crdName, Version and Group and then verifies
// if the given expectedInstanceName exist in the list.
func verifyCRDInSupervisor(ctx context.Context, f *framework.Framework, expectedInstanceName string,
	crdName string, crdVersion string, crdGroup string, isCreated bool) {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var instanceFound bool
	for _, crd := range list.Items {
		if crdName == "cnsnodevmattachments" {
			instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSNodeVMAttachment crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}

		}
		if crdName == "cnsvolumemetadatas" {
			instance := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}
		}
		if crdName == "cnsfileaccessconfigs" {
			instance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSFileAccessConfig crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			} else {
				ginkgo.By(fmt.Sprintf("Expecting CNSFileAccessConfig crd: %v, found: %v", expectedInstanceName, instance))
			}
		}
	}
	if isCreated {
		gomega.Expect(instanceFound).To(gomega.BeTrue())
	} else {
		gomega.Expect(instanceFound).To(gomega.BeFalse())
	}
}

// verifyCNSFileAccessConfigCRDInSupervisor is a helper method to check if a
// given crd is created/deleted in the supervisor cluster. This method will
// fetch the list of CRD Objects for a given crdName, Version and Group and then
// verifies if the given expectedInstanceName exist in the list.
func verifyCNSFileAccessConfigCRDInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string, isCreated bool) {
	// Adding an explicit wait time for the recounciler to refresh the status.
	time.Sleep(30 * time.Second)

	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var instanceFound bool
	for _, crd := range list.Items {
		if crdName == "cnsfileaccessconfigs" {
			instance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSFileAccessConfig crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			} else {
				framework.Logf("CRD is not matching : " + instance.Name)
			}
		}
	}
	if isCreated {
		gomega.Expect(instanceFound).To(gomega.BeTrue())
	} else {
		gomega.Expect(instanceFound).To(gomega.BeFalse())
	}
}

// waitTillCNSFileAccesscrdDeleted is a helper method to check if a
// given crd is created/deleted in the supervisor cluster. This method will
// fetch the list of CRD Objects for a given crdName, Version and Group and then
// verifies if the given expectedInstanceName exist in the list.
func waitTillCNSFileAccesscrdDeleted(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string, isCreated bool) error {
	return wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		framework.Logf("Waiting for crd %s to disappear", expectedInstanceName)

		k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
		cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dynamicClient, err := dynamic.NewForConfig(cfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}

		resourceClient := dynamicClient.Resource(gvr).Namespace("")
		list, err := resourceClient.List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if err != nil {
			framework.Logf("Error fetching the crds")
			return false, nil
		}

		found := false

		for _, crd := range list.Items {
			if crdName == "cnsfileaccessconfigs" {
				instance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
				err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if expectedInstanceName == instance.Name {
					framework.Logf("CNSFileAccessConfig crd still exists")
					found = true
					break
				}
			}
		}
		if !found {
			framework.Logf("CRD %s no longer exists", expectedInstanceName)
			return true, nil
		}

		return false, nil
	})
}

// verifyEntityReferenceForKubEntities takes context,client, pv, pvc and pod
// as inputs and verifies crdCNSVolumeMetadata is attached.
func verifyEntityReferenceForKubEntities(ctx context.Context, f *framework.Framework,
	client clientset.Interface, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, pod *v1.Pod) {
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	volumeID := pv.Spec.CSI.VolumeHandle
	// svcPVCName refers to PVC Name in the supervisor cluster.
	svcPVCName := volumeID
	volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
	gomega.Expect(volumeID).NotTo(gomega.BeEmpty())

	podUID := string(pod.UID)
	fmt.Println("Pod uuid :", podUID)
	fmt.Println("PVC name in SV", svcPVCName)
	pvcUID := string(pvc.GetUID())
	fmt.Println("PVC UUID in GC", pvcUID)
	gcClusterID := strings.Replace(svcPVCName, pvcUID, "", -1)

	fmt.Println("gcClusterId", gcClusterID)
	pvUID := string(pv.UID)
	fmt.Println("PV uuid", pvUID)

	verifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle, crdCNSVolumeMetadatas,
		crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
	verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID, crdCNSVolumeMetadatas,
		crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
	verifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID, crdCNSVolumeMetadatas,
		crdVersion, crdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
}

// verifyEntityReferenceInCRDInSupervisor is a helper method to check
// CnsVolumeMetadata CRDs exists and has expected labels.
func verifyEntityReferenceInCRDInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string,
	isCreated bool, volumeNames string, checkLabel bool, labels map[string]string, labelPresent bool) {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// This sleep is to make sure the entity reference is populated.
	time.Sleep(10 * time.Second)
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("expected instancename is : %v", expectedInstanceName)

	var instanceFound bool
	for _, crd := range list.Items {
		if crdName == "cnsvolumemetadatas" {
			instance := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Found CNSVolumeMetadata type crd instance: %v", instance.Name)
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v", instance, expectedInstanceName))
				vol := instance.Spec.VolumeNames
				if vol[0] == volumeNames {
					ginkgo.By(fmt.Sprintf("Entity reference for the volume: %v ,found in the CRD : %v",
						volumeNames, expectedInstanceName))
				}

				if checkLabel {
					ginkgo.By(fmt.Sprintf("Checking the cnsvolumemetadatas for the expected label %v", labels))
					labelsPresent := instance.Spec.Labels
					labelFound := reflect.DeepEqual(labelsPresent, labels)
					if labelFound {
						gomega.Expect(labelFound).To(gomega.BeTrue())
					} else {
						gomega.Expect(labelFound).To(gomega.BeFalse())
					}
				}
				instanceFound = true
				break
			}
		}
	}
	if isCreated {
		gomega.Expect(instanceFound).To(gomega.BeTrue())
	} else {
		gomega.Expect(instanceFound).To(gomega.BeFalse())
	}
}

// trimQuotes takes a quoted string as input and returns the same string unquoted.
func trimQuotes(str string) string {
	str = strings.TrimPrefix(str, "\"")
	str = strings.TrimSuffix(str, " ")
	str = strings.TrimSuffix(str, "\"")

	return str
}

// readConfigFromSecretString takes input string of the form:
//
//	[Global]
//	insecure-flag = "true"
//	cluster-id = "domain-c1047"
//	cluster-distribution = "CSI-Vanilla"
//	[VirtualCenter "wdc-rdops-vm09-dhcp-238-224.eng.vmware.com"]
//	user = "workload_storage_management-792c9cce-3cd2-4618-8853-52f521400e05@vsphere.local"
//	password = "qd?\\/\"K=O_<ZQw~s4g(S"
//	datacenters = "datacenter-1033"
//	port = "443"
//	[Snapshot]
//	global-max-snapshots-per-block-volume = 3
//
// Returns a de-serialized structured config data
func readConfigFromSecretString(cfg string) (e2eTestConfig, error) {
	var config e2eTestConfig
	key, value := "", ""
	lines := strings.Split(cfg, "\n")
	for index, line := range lines {
		if index == 0 {
			// Skip [Global].
			continue
		}
		words := strings.Split(line, " = ")
		if strings.Contains(words[0], "topology-categories=") {
			words = strings.Split(line, "=")
		}

		if len(words) == 1 {
			// Case Snapshot
			if strings.Contains(words[0], "Snapshot") {
				continue
			}
			if strings.Contains(words[0], "Labels") {
				continue
			}
			// Case VirtualCenter.
			words = strings.Split(line, " ")
			if strings.Contains(words[0], "VirtualCenter") {
				value = words[1]
				// Remove trailing '"]' characters from value.
				value = strings.TrimSuffix(value, "]")
				config.Global.VCenterHostname = trimQuotes(value)
				fmt.Printf("Key: VirtualCenter, Value: %s\n", value)
			}
			continue
		}
		key = words[0]
		value = trimQuotes(words[1])
		var strconvErr error
		switch key {
		case "insecure-flag":
			if strings.Contains(value, "true") {
				config.Global.InsecureFlag = true
			} else {
				config.Global.InsecureFlag = false
			}
		case "cluster-id":
			config.Global.ClusterID = value
		case "cluster-distribution":
			config.Global.ClusterDistribution = value
		case "user":
			config.Global.User = value
		case "password":
			config.Global.Password = value
		case "datacenters":
			config.Global.Datacenters = value
		case "port":
			config.Global.VCenterPort = value
		case "cnsregistervolumes-cleanup-intervalinmin":
			config.Global.CnsRegisterVolumesCleanupIntervalInMin, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "topology-categories":
			config.Labels.TopologyCategories = value
		case "global-max-snapshots-per-block-volume":
			config.Snapshot.GlobalMaxSnapshotsPerBlockVolume, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "csi-fetch-preferred-datastores-intervalinmin":
			config.Global.CSIFetchPreferredDatastoresIntervalInMin, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "targetvSANFileShareDatastoreURLs":
			config.Global.TargetvSANFileShareDatastoreURLs = value
		case "query-limit":
			config.Global.QueryLimit, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "list-volume-threshold":
			config.Global.ListVolumeThreshold, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())

		default:
			return config, fmt.Errorf("unknown key %s in the input string", key)
		}
	}
	return config, nil
}

// writeConfigToSecretString takes in a structured config data and serializes
// that into a string.
func writeConfigToSecretString(cfg e2eTestConfig) (string, error) {
	result := fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\ncluster-id = \"%s\"\ncluster-distribution = \"%s\"\n"+
		"csi-fetch-preferred-datastores-intervalinmin = %d\n"+"query-limit = \"%d\"\n"+
		"list-volume-threshold = \"%d\"\n\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"\n"+
		"targetvSANFileShareDatastoreURLs = \"%s\"\n\n"+
		"[Snapshot]\nglobal-max-snapshots-per-block-volume = %d\n\n"+
		"[Labels]\ntopology-categories = \"%s\"",
		cfg.Global.InsecureFlag, cfg.Global.ClusterID, cfg.Global.ClusterDistribution,
		cfg.Global.CSIFetchPreferredDatastoresIntervalInMin, cfg.Global.QueryLimit,
		cfg.Global.ListVolumeThreshold,
		cfg.Global.VCenterHostname, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort, cfg.Global.TargetvSANFileShareDatastoreURLs,
		cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume,
		cfg.Labels.TopologyCategories)
	return result, nil
}

// Function to create CnsRegisterVolume spec, with given FCD ID and PVC name.
func getCNSRegisterVolumeSpec(ctx context.Context, namespace string, fcdID string,
	vmdkPath string, persistentVolumeClaimName string,
	accessMode v1.PersistentVolumeAccessMode) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	var (
		cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume
	)
	framework.Logf("get CNSRegisterVolume spec")
	cnsRegisterVolume = &cnsregistervolumev1alpha1.CnsRegisterVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "cnsregvol-",
			Namespace:    namespace,
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName: persistentVolumeClaimName,
			AccessMode: v1.PersistentVolumeAccessMode(
				accessMode,
			),
		},
	}

	if vmdkPath != "" {
		cnsRegisterVolume.Spec.DiskURLPath = vmdkPath
	}

	if fcdID != "" {
		cnsRegisterVolume.Spec.VolumeID = fcdID
	}
	return cnsRegisterVolume
}

// Create CNS register volume.
func createCNSRegisterVolume(ctx context.Context, restConfig *rest.Config,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume) error {

	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Create CNSRegisterVolume")
	err = cnsOperatorClient.Create(ctx, cnsRegisterVolume)

	return err
}

// Query CNS Register volume. Returns true if the CNSRegisterVolume is
// available otherwise false.
func queryCNSRegisterVolume(ctx context.Context, restClientConfig *rest.Config,
	cnsRegistervolumeName string, namespace string) bool {
	isPresent := false
	framework.Logf("cleanUpCnsRegisterVolumeInstances: start")
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Get list of CnsRegisterVolume instances from all namespaces.
	cnsRegisterVolumesList := &cnsregistervolumev1alpha1.CnsRegisterVolumeList{}
	err = cnsOperatorClient.List(ctx, cnsRegisterVolumesList)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cns := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err = cnsOperatorClient.Get(ctx, pkgtypes.NamespacedName{Name: cnsRegistervolumeName, Namespace: namespace}, cns)
	if err == nil {
		framework.Logf("CNS RegisterVolume %s Found in the namespace  %s:", cnsRegistervolumeName, namespace)
		isPresent = true
	}

	return isPresent

}

// Verify Bi-directional referance of Pv and PVC in case of static volume
// provisioning.
func verifyBidirectionalReferenceOfPVandPVC(ctx context.Context, client clientset.Interface,
	pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume, fcdID string) {

	pvcName := pvc.GetName()
	pvcNamespace := pvc.GetNamespace()
	pvcvolume := pvc.Spec.VolumeName
	pvccapacity := pvc.Status.Capacity.StorageEphemeral().Size()

	pvName := pv.GetName()
	pvClaimRefName := pv.Spec.ClaimRef.Name
	pvClaimRefNamespace := pv.Spec.ClaimRef.Namespace
	pvcapacity := pv.Spec.Capacity.StorageEphemeral().Size()
	pvvolumeHandle := pv.Spec.PersistentVolumeSource.CSI.VolumeHandle

	if pvClaimRefName != pvcName && pvClaimRefNamespace != pvcNamespace {
		framework.Logf("PVC Name :%s PVC namespace : %s", pvcName, pvcNamespace)
		framework.Logf("PV Name :%s PVnamespace : %s", pvcName, pvcNamespace)
		framework.Failf("Mismatch in PV and PVC name and namespace")
	}

	if pvcvolume != pvName {
		framework.Failf("PVC volume :%s PV name : %s expected to be same", pvcvolume, pvName)
	}

	if pvvolumeHandle != fcdID {
		framework.Failf("Mismatch in PV volumeHandle:%s and the actual fcdID: %s", pvvolumeHandle, fcdID)
	}

	if pvccapacity != pvcapacity {
		framework.Failf("Mismatch in pv capacity:%d and pvc capacity: %d", pvcapacity, pvccapacity)
	}
}

// Get CNS register volume.
func getCNSRegistervolume(ctx context.Context, restClientConfig *rest.Config,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cns := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: cnsRegisterVolume.Name, Namespace: cnsRegisterVolume.Namespace}, cns)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return cns
}

// Update CNS register volume.
func updateCNSRegistervolume(ctx context.Context, restClientConfig *rest.Config,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume) *cnsregistervolumev1alpha1.CnsRegisterVolume {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = cnsOperatorClient.Update(ctx, cnsRegisterVolume)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return cnsRegisterVolume

}

// CreatePodByUserID with given claims based on node selector. This method is
// addition to CreatePod method. Here userID can be specified for pod user.
func CreatePodByUserID(client clientset.Interface, namespace string, nodeSelector map[string]string,
	pvclaims []*v1.PersistentVolumeClaim, isPrivileged bool, command string, userID int64) (*v1.Pod, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pod := GetPodSpecByUserID(namespace, nodeSelector, pvclaims, isPrivileged, command, userID)
	pod, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Pod creation failed")
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// Get fresh pod info.
	pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}
	return pod, nil
}

// GetPodSpecByUserID returns a pod definition based on the namespace.
// The pod references the PVC's name.
// This method is addition to MakePod method.
// Here userID can be specified for pod user.
func GetPodSpecByUserID(ns string, nodeSelector map[string]string, pvclaims []*v1.PersistentVolumeClaim,
	isPrivileged bool, command string, userID int64) *v1.Pod {
	if len(command) == 0 {
		command = "trap exit TERM; while true; do sleep 1; done"
	}
	podSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-tester-",
			Namespace:    ns,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "write-pod",
					Image:   busyBoxImageOnGcr,
					Command: []string{"/bin/sh"},
					Args:    []string{"-c", command},
					SecurityContext: &v1.SecurityContext{
						Privileged: &isPrivileged,
						RunAsUser:  &userID,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyOnFailure,
		},
	}
	var volumeMounts = make([]v1.VolumeMount, len(pvclaims))
	var volumes = make([]v1.Volume, len(pvclaims))
	for index, pvclaim := range pvclaims {
		volumename := fmt.Sprintf("volume%v", index+1)
		volumeMounts[index] = v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename}
		volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
	}
	podSpec.Spec.Containers[0].VolumeMounts = volumeMounts
	podSpec.Spec.Volumes = volumes
	if nodeSelector != nil {
		podSpec.Spec.NodeSelector = nodeSelector
	}
	return podSpec
}

// writeDataOnFileFromPod writes specified data from given Pod at the given.
func writeDataOnFileFromPod(namespace string, podName string, filePath string, data string) {
	_, err := framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
		podName, "--", "/bin/sh", "-c", fmt.Sprintf(" echo %s >  %s ", data, filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// readFileFromPod read data from given Pod and the given file.
func readFileFromPod(namespace string, podName string, filePath string) string {
	output, err := framework.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
		podName, "--", "/bin/sh", "-c", fmt.Sprintf("less  %s", filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return output
}

// getPersistentVolumeClaimSpecFromVolume gets vsphere persistent volume spec
// with given selector labels and binds it to given pv.
func getPersistentVolumeClaimSpecFromVolume(namespace string, pvName string,
	labels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// getPersistentVolumeSpecFromVolume gets static PV volume spec with given
// Volume ID, Reclaim Policy and labels.
func getPersistentVolumeSpecFromVolume(volumeID string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
	labels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: volumeID,
				FSType:       "nfs4",
				ReadOnly:     true,
			},
		},
		Prebind: nil,
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
			Annotations:  annotations,
			Labels:       labels,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			ClaimRef:         claimRef,
			StorageClassName: "",
		},
		Status: v1.PersistentVolumeStatus{},
	}
	return pv
}

// DeleteStatefulPodAtIndex deletes pod given index in the desired statefulset.
func DeleteStatefulPodAtIndex(client clientset.Interface, index int, ss *appsv1.StatefulSet) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	name := fmt.Sprintf("%v-%v", ss.Name, index)
	noGrace := int64(0)
	err := client.CoreV1().Pods(ss.Namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &noGrace})
	if err != nil {
		framework.Failf("Failed to delete stateful pod %v for StatefulSet %v/%v: %v", name, ss.Namespace, ss.Name, err)
	}

}

// getClusterComputeResource returns the clusterComputeResource and vSANClient.
func getClusterComputeResource(ctx context.Context, vs *vSphere) ([]*object.ClusterComputeResource, *VsanClient) {
	var err error
	if clusterComputeResource == nil {
		clusterComputeResource, vsanHealthClient, err = getClusterName(ctx, vs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return clusterComputeResource, vsanHealthClient
}

// findIP returns the IP from the input string.
func findIP(input string) string {
	numBlock := "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])"
	regexPattern := numBlock + "\\." + numBlock + "\\." + numBlock + "\\." + numBlock
	regEx := regexp.MustCompile(regexPattern)
	return regEx.FindString(input)
}

// getHosts returns list of hosts and it takes clusterComputeResource as input.
// This method is used by WCP and GC tests.
func getHosts(ctx context.Context, clusterComputeResource []*object.ClusterComputeResource) []*object.HostSystem {
	var err error
	if hosts == nil {
		computeCluster := os.Getenv(envComputeClusterName)
		if computeCluster == "" {
			if guestCluster {
				computeCluster = "compute-cluster"
			} else if supervisorCluster {
				computeCluster = "wcp-app-platform-sanity-cluster"
			}
			framework.Logf("Default cluster is chosen for test")
		}
		for _, cluster := range clusterComputeResource {
			framework.Logf("clusterComputeResource %v", clusterComputeResource)
			if strings.Contains(cluster.Name(), computeCluster) {
				hosts, err = cluster.Hosts(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	gomega.Expect(hosts).NotTo(gomega.BeNil())
	return hosts
}

// waitForAllHostsToBeUp will check and wait till the host is reachable.
func waitForAllHostsToBeUp(ctx context.Context, vs *vSphere) {
	clusterComputeResource, _ = getClusterComputeResource(ctx, vs)
	hosts = getHosts(ctx, clusterComputeResource)
	framework.Logf("host information %v", hosts)
	for index := range hosts {
		ip := findIP(hosts[index].String())
		err := waitForHostToBeUp(ip)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// psodHostWithPv methods finds the esx host where pv is residing and psods it.
// It uses VsanObjIndentities and QueryVsanObjects apis to achieve it and
// returns the host ip.
func psodHostWithPv(ctx context.Context, vs *vSphere, pvName string) string {
	ginkgo.By("VsanObjIndentities")
	framework.Logf("pvName %v", pvName)
	vsanObjuuid := VsanObjIndentities(ctx, &e2eVSphere, pvName)
	framework.Logf("vsanObjuuid %v", vsanObjuuid)
	gomega.Expect(vsanObjuuid).NotTo(gomega.BeNil())

	ginkgo.By("Get host info using queryVsanObj")
	hostInfo := queryVsanObj(ctx, &e2eVSphere, vsanObjuuid)
	framework.Logf("vsan object ID %v", hostInfo)
	gomega.Expect(hostInfo).NotTo(gomega.BeEmpty())
	hostIP := e2eVSphere.getHostUUID(ctx, hostInfo)
	framework.Logf("hostIP %v", hostIP)
	gomega.Expect(hostIP).NotTo(gomega.BeEmpty())

	ginkgo.By("PSOD")
	sshCmd := fmt.Sprintf("vsish -e set /config/Misc/intOpts/BlueScreenTimeout %s", psodTime)
	op, err := runCommandOnESX("root", hostIP, sshCmd)
	framework.Logf(op)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Injecting PSOD ")
	psodCmd := "vsish -e set /reliability/crashMe/Panic 1"
	op, err = runCommandOnESX("root", hostIP, psodCmd)
	framework.Logf(op)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	return hostIP
}

// VsanObjIndentities returns the vsanObjectsUUID.
func VsanObjIndentities(ctx context.Context, vs *vSphere, pvName string) string {
	var vsanObjUUID string
	computeCluster := os.Getenv(envComputeClusterName)
	if computeCluster == "" {
		if guestCluster {
			computeCluster = "compute-cluster"
		} else if supervisorCluster {
			computeCluster = "wcp-app-platform-sanity-cluster"
		}
		framework.Logf("Default cluster is chosen for test")
	}
	clusterComputeResource, vsanHealthClient = getClusterComputeResource(ctx, vs)

	for _, cluster := range clusterComputeResource {
		if strings.Contains(cluster.Name(), computeCluster) {
			clusterConfig, err := vsanHealthClient.VsanQueryObjectIdentities(ctx, cluster.Reference())
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

// queryVsanObj takes vsanObjuuid as input and resturns vsanObj info such as
// hostUUID.
func queryVsanObj(ctx context.Context, vs *vSphere, vsanObjuuid string) string {
	c := newClient(ctx, vs)
	datacenter := e2eVSphere.Config.Global.Datacenters

	vsanHealthClient, err := newVsanHealthSvcClient(ctx, c.Client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	finder := find.NewFinder(vsanHealthClient.vim25Client, false)
	dc, err := finder.Datacenter(ctx, datacenter)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	finder.SetDatacenter(dc)
	result, err := vsanHealthClient.QueryVsanObjects(ctx, []string{vsanObjuuid}, vs)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return result
}

// hostLogin methods sets the ESX host password.
func hostLogin(user, instruction string, questions []string, echos []bool) (answers []string, err error) {
	answers = make([]string, len(questions))
	nimbusGeneratedEsxPwd := GetAndExpectStringEnvVar(nimbusEsxPwd)
	for n := range questions {
		answers[n] = nimbusGeneratedEsxPwd
	}
	return answers, nil
}

// runCommandOnESX executes ssh commands on the give ESX host and returns the bash
// result.
func runCommandOnESX(username string, addr string, cmd string) (string, error) {
	// Authentication.
	config := &ssh.ClientConfig{
		User:            username,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.KeyboardInteractive(hostLogin),
		},
	}

	result := fssh.Result{Host: addr, Cmd: cmd}

	// Connect.
	client, err := ssh.Dial("tcp", net.JoinHostPort(addr, sshdPort), config)
	if err != nil {
		framework.Logf("connection failed due to %v", err)
		return "", err
	}
	// Create a session. It is one session per command.
	session, err := client.NewSession()
	if err != nil {
		framework.Logf("session creation failed due to %v", err)
		return "", err
	}
	defer session.Close()

	// Run the command.
	code := 0
	var bytesStdout, bytesStderr bytes.Buffer
	session.Stdout, session.Stderr = &bytesStdout, &bytesStderr
	if err = session.Run(cmd); err != nil {
		if exiterr, ok := err.(*ssh.ExitError); ok {
			// If we got an ExitError and the exit code is nonzero, we'll
			// consider the SSH itself successful but cmd failed on the host.
			if code = exiterr.ExitStatus(); code != 0 {
				err = nil
			}
		}
		if exiterr, ok := err.(*ssh.ExitMissingError); ok {
			/* If we got an  ExitMissingError and the exit code is zero, we'll
			consider the SSH itself successful and cmd executed successfully on the host.
			If  exit code is non zero we'll consider the SSH is successful but
			cmd failed on the host. */
			framework.Logf(exiterr.Error())
			if code == 0 {
				err = nil
			} else {
				err = fmt.Errorf("failed running `%s` on %s@%s: '%v'", cmd, config.User, addr, err)
			}
		} else {
			err = fmt.Errorf("failed running `%s` on %s@%s: '%v'", cmd, config.User, addr, err)
		}
	}
	result.Stdout = bytesStdout.String()
	result.Stderr = bytesStderr.String()
	result.Code = code
	return result.Stdout, err
}

// stopHostDOnHost executes hostd stop service commands on the given ESX host
func stopHostDOnHost(ctx context.Context, addr string) {
	framework.Logf("Stopping hostd service on the host  %s ...", addr)
	stopHostCmd := fmt.Sprintf("/etc/init.d/hostd %s", stopOperation)
	_, err := runCommandOnESX("root", addr, stopHostCmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = waitForHostConnectionState(ctx, addr, "notResponding")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// startHostDOnHost executes hostd start service commands on the given ESX host
func startHostDOnHost(ctx context.Context, addr string) {
	framework.Logf("Starting hostd service on the host  %s ...", addr)
	startHostDCmd := fmt.Sprintf("/etc/init.d/hostd %s", startOperation)
	_, err := runCommandOnESX("root", addr, startHostDCmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = waitForHostConnectionState(ctx, addr, "connected")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	output := getHostDStatusOnHost(addr)
	gomega.Expect(strings.Contains(output, "hostd is running.")).NotTo(gomega.BeFalse())
}

// getHostDStatusOnHost executes hostd status service commands on the given ESX host
func getHostDStatusOnHost(addr string) string {
	framework.Logf("Running status check on hostd service for the host  %s ...", addr)
	statusHostDCmd := fmt.Sprintf("/etc/init.d/hostd %s", statusOperation)
	output, err := runCommandOnESX("root", addr, statusHostDCmd)
	framework.Logf("hostd status command output is : " + output)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return output
}

// getPersistentVolumeSpecWithStorageclass is to create PV volume spec with
// given FCD ID, Reclaim Policy and labels.
func getPersistentVolumeSpecWithStorageclass(volumeHandle string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, storageClass string,
	labels map[string]string, sizeOfDisk string) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: volumeHandle,
				ReadOnly:     false,
				FSType:       "ext4",
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(sizeOfDisk),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:         claimRef,
			StorageClassName: storageClass,
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// getPVCSpecWithPVandStorageClass is to create PVC spec with given PV, storage
// class and label details.
func getPVCSpecWithPVandStorageClass(pvcName string, namespace string, labels map[string]string,
	pvName string, storageclass string, sizeOfDisk string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvcName,
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(sizeOfDisk),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &storageclass,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// waitForEvent waits for and event with specified message substr for a given
// object name.
func waitForEvent(ctx context.Context, client clientset.Interface,
	namespace string, substr string, name string) error {
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		eventList, err := client.CoreV1().Events(namespace).List(ctx,
			metav1.ListOptions{FieldSelector: "involvedObject.name=" + name})
		if err != nil {
			return false, err
		}
		for _, item := range eventList.Items {
			if strings.Contains(item.Message, substr) {
				framework.Logf("Found event %v", item)
				return true, nil
			}
		}
		return false, nil
	})
	return waitErr
}

// bringSvcK8sAPIServerDown function moves the static kube-apiserver.yaml out
// of k8's manifests directory. It takes VC IP and SV K8's master IP as input.
func bringSvcK8sAPIServerDown(vc string) error {
	file := "master.txt"
	token := "token.txt"
	// Note: /usr/lib/vmware-wcp/decryptK8Pwd.py is not an officially supported
	// API and may change at any time.
	sshCmd := fmt.Sprintf("/usr/lib/vmware-wcp/decryptK8Pwd.py > %s", file)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err := fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf("(awk 'FNR == 7 {print $2}' %s) > %s", file, token)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err = fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf(
		"sshpass -f %s ssh root@$(awk 'FNR == 6 {print $2}' master.txt) -o 'StrictHostKeyChecking no' 'mv %s/%s /root'",
		token, kubeAPIPath, kubeAPIfile)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err = fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	time.Sleep(kubeAPIRecoveryTime)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// bringSvcK8sAPIServerUp function moves the static kube-apiserver.yaml to
// k8's manifests directory. It takes VC IP and SV K8's master IP as input.
func bringSvcK8sAPIServerUp(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, vc, healthStatus string) error {
	sshCmd := fmt.Sprintf("sshpass -f token.txt ssh root@$(awk 'FNR == 6 {print $2}' master.txt) "+
		"-o 'StrictHostKeyChecking no' 'mv /root/%s %s'", kubeAPIfile, kubeAPIPath)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err := fssh.SSH(sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	ginkgo.By(fmt.Sprintf("polling for %v minutes...", healthStatusPollTimeout))
	err = pvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatus)
	if err != nil {
		return err
	}
	return nil
}

// pvcHealthAnnotationWatcher polls the health status of pvc and returns error
// if any.
func pvcHealthAnnotationWatcher(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, healthStatus string) error {
	framework.Logf("Waiting for health annotation for pvclaim %v", pvclaim.Name)
	waitErr := wait.Poll(healthStatusPollInterval, healthStatusPollTimeout, func() (bool, error) {
		framework.Logf("wait for next poll %v", healthStatusPollInterval)
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if pvc.Annotations[pvcHealthAnnotation] == healthStatus {
			framework.Logf("health annonation added :%v", pvc.Annotations[pvcHealthAnnotation])
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// waitForHostToBeUp will check the status of hosts and also wait for
// pollTimeout minutes to make sure host is reachable.
func waitForHostToBeUp(ip string, pollInfo ...time.Duration) error {
	framework.Logf("checking host status of %v", ip)
	pollTimeOut := healthStatusPollTimeout
	pollInterval := 30 * time.Second
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
	waitErr := wait.Poll(pollInterval, pollTimeOut, func() (bool, error) {
		_, err := net.DialTimeout("tcp", ip+":22", dialTimeout)
		if err != nil {
			framework.Logf("host %s unreachable, error: %s", ip, err.Error())
			return false, nil
		}
		framework.Logf("host %s reachable", ip)
		return true, nil
	})
	return waitErr
}

// waitForNamespaceToGetDeleted waits for a namespace to get deleted or until
// timeout occurs, whichever comes first.
func waitForNamespaceToGetDeleted(ctx context.Context, c clientset.Interface,
	namespaceToDelete string, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for namespace %s to get deleted", timeout, namespaceToDelete)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		namespace, err := c.CoreV1().Namespaces().Get(ctx, namespaceToDelete, metav1.GetOptions{})
		if err == nil {
			framework.Logf("Namespace %s found and status=%s (%v)", namespaceToDelete, namespace.Status, time.Since(start))
			continue
		}
		if apierrors.IsNotFound(err) {
			framework.Logf("namespace %s was removed", namespaceToDelete)
			return nil
		}
		framework.Logf("Get namespace %s is failed, ignoring for %v: %v", namespaceToDelete, Poll, err)
	}
	return fmt.Errorf("namespace %s still exists within %v", namespaceToDelete, timeout)
}

// waitForCNSRegisterVolumeToGetCreated waits for a cnsRegisterVolume to get
// created or until timeout occurs, whichever comes first.
func waitForCNSRegisterVolumeToGetCreated(ctx context.Context, restConfig *rest.Config, namespace string,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for CnsRegisterVolume %s to get created", timeout, cnsRegisterVolume)

	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		cnsRegisterVolume = getCNSRegistervolume(ctx, restConfig, cnsRegisterVolume)
		flag := cnsRegisterVolume.Status.Registered
		if !flag {
			continue
		} else {
			return nil
		}
	}

	return fmt.Errorf("cnsRegisterVolume %s creation is failed within %v", cnsRegisterVolumeName, timeout)
}

// waitForCNSRegisterVolumeToGetDeleted waits for a cnsRegisterVolume to get
// deleted or until timeout occurs, whichever comes first.
func waitForCNSRegisterVolumeToGetDeleted(ctx context.Context, restConfig *rest.Config, namespace string,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for cnsRegisterVolume %s to get deleted", timeout, cnsRegisterVolume)

	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		flag := queryCNSRegisterVolume(ctx, restConfig, cnsRegisterVolumeName, namespace)
		if flag {
			framework.Logf("CnsRegisterVolume %s is not yet deleted. Deletion flag status  =%s (%v)",
				cnsRegisterVolumeName, flag, time.Since(start))
			continue
		}
		return nil
	}

	return fmt.Errorf("CnsRegisterVolume %s deletion is failed within %v", cnsRegisterVolumeName, timeout)
}

// getK8sMasterIP gets k8s master ip in vanilla setup.
func getK8sMasterIPs(ctx context.Context, client clientset.Interface) []string {
	var err error
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var k8sMasterIPs []string
	for _, node := range nodes.Items {
		if strings.Contains(node.Name, "master") || strings.Contains(node.Name, "control") {
			addrs := node.Status.Addresses
			for _, addr := range addrs {
				if addr.Type == v1.NodeExternalIP && (net.ParseIP(addr.Address)).To4() != nil {
					k8sMasterIPs = append(k8sMasterIPs, addr.Address)
				}
			}
		}
	}
	gomega.Expect(k8sMasterIPs).NotTo(gomega.BeEmpty(), "Unable to find k8s control plane IP")
	return k8sMasterIPs
}

// toggleCSIMigrationFeatureGatesOnKubeControllerManager adds/removes
// CSIMigration and CSIMigrationvSphere feature gates to/from
// kube-controller-manager.
func toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx context.Context,
	client clientset.Interface, add bool) error {
	nimbusGeneratedK8sVmPwd := GetAndExpectStringEnvVar(nimbusK8sVmPwd)
	v, err := client.Discovery().ServerVersion()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	v1, err := version.NewVersion(v.GitVersion)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	v2, err := version.NewVersion("v1.25.0")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if v1.LessThan(v2) {
		sshCmd := ""
		if !vanillaCluster {
			return fmt.Errorf(
				"'toggleCSIMigrationFeatureGatesToKubeControllerManager' is implemented for vanilla cluster alone")
		}
		if add {
			sshCmd =
				"sed -i -e 's/CSIMigration=false,CSIMigrationvSphere=false/CSIMigration=true,CSIMigrationvSphere=true/g' " +
					kcmManifest
		} else {
			sshCmd = "sed -i '/CSIMigration/d' " + kcmManifest
		}
		grepCmd := "grep CSIMigration " + kcmManifest
		k8sMasterIPs := getK8sMasterIPs(ctx, client)
		for _, k8sMasterIP := range k8sMasterIPs {
			framework.Logf("Invoking command '%v' on host %v", grepCmd, k8sMasterIP)
			sshClientConfig := &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(nimbusGeneratedK8sVmPwd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
			result, err := sshExec(sshClientConfig, k8sMasterIP, grepCmd)

			if err != nil {
				fssh.LogResult(result)
				return fmt.Errorf("command failed/couldn't execute command: %s on host: %v , error: %s",
					grepCmd, k8sMasterIP, err)
			}
			if result.Code != 0 {
				if add {
					// nolint:misspell
					sshCmd = "gawk -i inplace '/--bind-addres/ " +
						"{ print; print \"    - --feature-gates=CSIMigration=true,CSIMigrationvSphere=true\"; next }1' " +
						kcmManifest
				} else {
					return nil
				}
			}
			framework.Logf("Invoking command %v on host %v", sshCmd, k8sMasterIP)
			result, err = sshExec(sshClientConfig, k8sMasterIP, sshCmd)
			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s", sshCmd, k8sMasterIP, err)
			}
			restartKubeletCmd := "systemctl restart kubelet"
			framework.Logf("Invoking command '%v' on host %v", restartKubeletCmd, k8sMasterIP)
			result, err = sshExec(sshClientConfig, k8sMasterIP, restartKubeletCmd)
			if err != nil && result.Code != 0 {
				fssh.LogResult(result)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", restartKubeletCmd,
						k8sMasterIP))
			}
		}
		// Sleeping for two seconds so that the change made to manifest file is
		// recognised.
		time.Sleep(2 * time.Second)

		framework.Logf(
			"Waiting for 'kube-controller-manager' controller pod to come up within %v seconds", pollTimeout*2)
		label := labels.SelectorFromSet(labels.Set(map[string]string{"component": "kube-controller-manager"}))
		_, err = fpod.WaitForPodsWithLabelRunningReady(
			client, kubeSystemNamespace, label, len(k8sMasterIPs), pollTimeout*2)
		if err == nil {
			framework.Logf("'kube-controller-manager' controller pod is up and ready within %v seconds", pollTimeout*2)
		} else {
			framework.Logf(
				"'kube-controller-manager' controller pod is not up and/or ready within %v seconds", pollTimeout*2)
		}
	}
	return err
}

// sshExec runs a command on the host via ssh.
func sshExec(sshClientConfig *ssh.ClientConfig, host string, cmd string) (fssh.Result, error) {
	result := fssh.Result{Host: host, Cmd: cmd}
	sshClient, err := ssh.Dial("tcp", host+":22", sshClientConfig)
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

// createPod with given claims based on node selector.
func createPod(client clientset.Interface, namespace string, nodeSelector map[string]string,
	pvclaims []*v1.PersistentVolumeClaim, isPrivileged bool, command string) (*v1.Pod, error) {
	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, isPrivileged, command)
	pod.Spec.Containers[0].Image = busyBoxImageOnGcr
	pod, err := client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// Get fresh pod info.
	pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}
	return pod, nil
}

// createDeployment create a deployment with 1 replica for given pvcs and node
// selector.
func createDeployment(ctx context.Context, client clientset.Interface, replicas int32,
	podLabels map[string]string, nodeSelector map[string]string, namespace string,
	pvclaims []*v1.PersistentVolumeClaim, command string, isPrivileged bool, image string) (*appsv1.Deployment, error) {
	if len(command) == 0 {
		command = "trap exit TERM; while true; do sleep 1; done"
	}
	zero := int64(0)
	deploymentName := "deployment-" + string(uuid.NewUUID())
	deploymentSpec := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
				},
				Spec: v1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers: []v1.Container{
						{
							Name:    "write-pod",
							Image:   image,
							Command: []string{"/bin/sh"},
							Args:    []string{"-c", command},
							SecurityContext: &v1.SecurityContext{
								Privileged: &isPrivileged,
							},
						},
					},
					RestartPolicy: v1.RestartPolicyAlways,
				},
			},
		},
	}
	var volumeMounts = make([]v1.VolumeMount, len(pvclaims))
	var volumes = make([]v1.Volume, len(pvclaims))
	for index, pvclaim := range pvclaims {
		volumename := fmt.Sprintf("volume%v", index+1)
		volumeMounts[index] = v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename}
		volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
	}
	deploymentSpec.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
	deploymentSpec.Spec.Template.Spec.Volumes = volumes
	if nodeSelector != nil {
		deploymentSpec.Spec.Template.Spec.NodeSelector = nodeSelector
	}
	deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("deployment %q Create API error: %v", deploymentSpec.Name, err)
	}
	framework.Logf("Waiting deployment %q to complete", deploymentSpec.Name)
	err = fdep.WaitForDeploymentComplete(client, deployment)
	if err != nil {
		return nil, fmt.Errorf("deployment %q failed to complete: %v", deploymentSpec.Name, err)
	}
	return deployment, nil
}

// createPodForFSGroup helps create pod with fsGroup.
func createPodForFSGroup(client clientset.Interface, namespace string,
	nodeSelector map[string]string, pvclaims []*v1.PersistentVolumeClaim,
	isPrivileged bool, command string, fsGroup *int64, runAsUser *int64) (*v1.Pod, error) {
	if len(command) == 0 {
		command = "trap exit TERM; while true; do sleep 1; done"
	}
	if fsGroup == nil {
		fsGroup = func(i int64) *int64 {
			return &i
		}(1000)
	}
	if runAsUser == nil {
		runAsUser = func(i int64) *int64 {
			return &i
		}(2000)
	}

	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, isPrivileged, command)
	pod.Spec.Containers[0].Image = busyBoxImageOnGcr
	pod.Spec.SecurityContext = &v1.PodSecurityContext{
		RunAsUser: runAsUser,
		FSGroup:   fsGroup,
	}
	pod, err := client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %v", pod.Name, err)
	}
	// Get fresh pod info.
	pod, err = client.CoreV1().Pods(namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %v", err)
	}
	return pod, nil
}

// getPersistentVolumeSpecForFileShare returns the PersistentVolume spec.
func getPersistentVolumeSpecForFileShare(fileshareID string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, labels map[string]string,
	accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	pv := getPersistentVolumeSpec(fileshareID, persistentVolumeReclaimPolicy, labels, nfs4FSType)
	pv.Spec.AccessModes = []v1.PersistentVolumeAccessMode{accessMode}
	return pv
}

// getPersistentVolumeClaimSpecForFileShare return the PersistentVolumeClaim
// spec in the specified namespace.
func getPersistentVolumeClaimSpecForFileShare(namespace string, labels map[string]string,
	pvName string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	pvc := getPersistentVolumeClaimSpec(namespace, labels, pvName)
	pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{accessMode}
	return pvc
}

// deleteFcdWithRetriesForSpecificErr method to retry fcd deletion when a
// specific error is encountered.
func deleteFcdWithRetriesForSpecificErr(ctx context.Context, fcdID string,
	dsRef vim25types.ManagedObjectReference, errsToIgnore []string, errsToContinue []string) error {
	var err error
	waitErr := wait.PollImmediate(poll*15, pollTimeout, func() (bool, error) {
		framework.Logf("Trying to delete FCD: %s", fcdID)
		err = e2eVSphere.deleteFCD(ctx, fcdID, dsRef)
		if err != nil {
			for _, errToIgnore := range errsToIgnore {
				if strings.Contains(err.Error(), errToIgnore) {
					// In FCD, there is a background thread that makes calls to host
					// to sync datastore every minute.
					framework.Logf("Hit error '%s' while trying to delete FCD: %s, will retry after %v seconds ...",
						err.Error(), fcdID, poll*15)
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

// getDefaultDatastore returns default datastore.
func getDefaultDatastore(ctx context.Context, forceRefresh ...bool) *object.Datastore {
	refresh := false
	if len(forceRefresh) > 0 {
		refresh = forceRefresh[0]
	}
	if defaultDatastore == nil || refresh {
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
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
			framework.Logf("Looking for default datastore in DC: " + dc)
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			if err == nil {
				framework.Logf("Datstore found for DS URL:" + datastoreURL)
				break
			}
		}
		gomega.Expect(defaultDatastore).NotTo(gomega.BeNil())
	}

	return defaultDatastore
}

// setClusterDistribution sets the input cluster-distribution in
// vsphere-config-secret.
func setClusterDistribution(ctx context.Context, client clientset.Interface, clusterDistribution string) {
	framework.Logf("Cluster distribution to set is = %s", clusterDistribution)

	// Get the current cluster-distribution value from secret.
	currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Secret Name is %s", currentSecret.Name)

	// Read and map the content of csi-vsphere.conf to a variable.
	originalConf := string(currentSecret.Data[vSphereCSIConf])
	cfg, err := readConfigFromSecretString(originalConf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Current value of cluster-distribution is.
	framework.Logf("Cluster-distribution value before modifying is = %s", cfg.Global.ClusterDistribution)

	// Check if the cluster-distribution value is as required or reset.
	if cfg.Global.ClusterDistribution != clusterDistribution {
		// Modify csi-vsphere.conf file.
		configContent := `[Global]
insecure-flag = "%t"
cluster-id = "%s"
cluster-distribution = "%s"

[VirtualCenter "%s"]
user = "%s"
password = "%s"
datacenters = "%s"
port = "%s"

[Snapshot]
global-max-snapshots-per-block-volume = %d`

		modifiedConf := fmt.Sprintf(configContent, cfg.Global.InsecureFlag, cfg.Global.ClusterID,
			clusterDistribution, cfg.Global.VCenterHostname, cfg.Global.User, cfg.Global.Password,
			cfg.Global.Datacenters, cfg.Global.VCenterPort, cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume)

		// Set modified csi-vsphere.conf file and update.
		framework.Logf("Updating the secret")
		currentSecret.Data[vSphereCSIConf] = []byte(modifiedConf)
		_, err := client.CoreV1().Secrets(csiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// TODO: Adding a explicit wait of two min for the Cluster-distribution to
		// reflect latest value. This should be replaced with a polling mechanism
		// to watch on csi-vsphere.conf inside the CSI containers.
		time.Sleep(time.Duration(2 * time.Minute))

		framework.Logf("Cluster distribution value is now set to = %s", clusterDistribution)

	} else {
		framework.Logf("Cluster-distribution value is already as expected, no changes done. Value is %s",
			cfg.Global.ClusterDistribution)
	}
}

// toggleCSIMigrationFeatureGatesOnK8snodes to toggle CSI migration feature
// gates on kublets for worker nodes.
func toggleCSIMigrationFeatureGatesOnK8snodes(ctx context.Context, client clientset.Interface, shouldEnable bool,
	namespace string) {

	v, err := client.Discovery().ServerVersion()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	v1, err := version.NewVersion(v.GitVersion)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	v2, err := version.NewVersion("v1.25.0")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if v1.LessThan(v2) {
		var err error
		var found bool
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, node := range nodes.Items {
			if strings.Contains(node.Name, "master") || strings.Contains(node.Name, "control") {
				continue
			}
			found = isCSIMigrationFeatureGatesEnabledOnKubelet(ctx, client, node.Name)
			if found == shouldEnable {
				continue
			}
			dh := drain.Helper{
				Ctx:                 ctx,
				Client:              client,
				Force:               true,
				IgnoreAllDaemonSets: true,
				Out:                 ginkgo.GinkgoWriter,
				ErrOut:              ginkgo.GinkgoWriter,
			}
			ginkgo.By("Cordoning of node: " + node.Name)
			err = drain.RunCordonOrUncordon(&dh, &node, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Draining of node: " + node.Name)
			err = drain.RunNodeDrain(&dh, node.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Modifying feature gates in kubelet config yaml of node: " + node.Name)
			nodeIP := getK8sNodeIP(&node)
			toggleCSIMigrationFeatureGatesOnkublet(ctx, client, nodeIP, shouldEnable)
			ginkgo.By("Wait for feature gates update on the k8s CSI node: " + node.Name)
			err = waitForCSIMigrationFeatureGatesToggleOnkublet(ctx, client, node.Name, shouldEnable)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Uncordoning of node: " + node.Name)
			err = drain.RunCordonOrUncordon(&dh, &node, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		pods, err := fpod.GetPodsInNamespace(client, namespace, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, namespace, int32(len(pods)), 0, pollTimeout*2, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// isCSIMigrationFeatureGatesEnabledOnKubelet checks whether CSIMigration
// Feature Gates are enabled on CSI Node.
func isCSIMigrationFeatureGatesEnabledOnKubelet(ctx context.Context, client clientset.Interface, nodeName string) bool {
	csinode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	found := false
	for annotation, value := range csinode.Annotations {
		framework.Logf("Annotation seen on CSI node - %s:%s", annotation, value)
		if annotation == migratedPluginAnnotation && strings.Contains(value, vcpProvisionerName) {
			found = true
			break
		}
	}
	return found
}

// waitForCSIMigrationFeatureGatesToggleOnkublet wait for CSIMigration Feature
// Gates toggle result on the csinode.
func waitForCSIMigrationFeatureGatesToggleOnkublet(ctx context.Context,
	client clientset.Interface, nodeName string, added bool) error {
	var found bool
	waitErr := wait.PollImmediate(poll*5, pollTimeout, func() (bool, error) {
		csinode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		found = false
		for annotation, value := range csinode.Annotations {
			framework.Logf("Annotation seen on CSI node - %s:%s", annotation, value)
			if annotation == migratedPluginAnnotation && strings.Contains(value, vcpProvisionerName) {
				found = true
				break
			}
		}
		if added && found {
			return true, nil
		}
		if !added && !found {
			return true, nil
		}
		return false, nil
	})
	return waitErr
}

// toggleCSIMigrationFeatureGatesOnkublet adds/remove CSI migration feature
// gates to kubelet config yaml in given k8s node.
func toggleCSIMigrationFeatureGatesOnkublet(ctx context.Context,
	client clientset.Interface, nodeIP string, shouldAdd bool) {
	grepCmd := "grep CSIMigration " + kubeletConfigYaml
	framework.Logf("Invoking command '%v' on host %v", grepCmd, nodeIP)
	nimbusGeneratedK8sVmPwd := GetAndExpectStringEnvVar(nimbusK8sVmPwd)
	sshClientConfig := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password(nimbusGeneratedK8sVmPwd),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	result, err := sshExec(sshClientConfig, nodeIP, grepCmd)
	if err != nil {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", grepCmd, nodeIP))
	}

	var sshCmd string
	if result.Code != 0 && shouldAdd {
		// Please don't change alignment in below assignment.
		sshCmd = `echo "featureGates:
  {
    "CSIMigration": true,
	"CSIMigrationvSphere": true
  }" >>` + kubeletConfigYaml
	} else if result.Code == 0 && !shouldAdd {
		sshCmd = fmt.Sprintf("head -n -5 %s > tmp.txt && mv tmp.txt %s", kubeletConfigYaml, kubeletConfigYaml)
	} else {
		return
	}
	framework.Logf("Invoking command '%v' on host %v", sshCmd, nodeIP)
	result, err = sshExec(sshClientConfig, nodeIP, sshCmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", sshCmd, nodeIP))
	}
	restartKubeletCmd := "systemctl daemon-reload && systemctl restart kubelet"
	framework.Logf("Invoking command '%v' on host %v", restartKubeletCmd, nodeIP)
	result, err = sshExec(sshClientConfig, nodeIP, restartKubeletCmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", restartKubeletCmd, nodeIP))
	}
}

// getK8sNodeIP returns the IP for the given k8s node.
func getK8sNodeIP(node *v1.Node) string {
	var address string
	addrs := node.Status.Addresses
	for _, addr := range addrs {
		if addr.Type == v1.NodeExternalIP && (net.ParseIP(addr.Address)).To4() != nil {
			address = addr.Address
			break
		}
	}
	gomega.Expect(address).NotTo(gomega.BeNil(), "Unable to find IP for node: "+node.Name)
	return address
}

// expectedAnnotation polls for the given annotation in pvc and returns error
// if its not present.
func expectedAnnotation(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, annotation string) error {
	framework.Logf("Waiting for health annotation for pvclaim %v", pvclaim.Name)
	waitErr := wait.Poll(healthStatusPollInterval, pollTimeout, func() (bool, error) {
		framework.Logf("wait for next poll %v", healthStatusPollInterval)
		pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for pvcAnnotation := range pvc.Annotations {
			if pvcAnnotation == annotation {
				return true, nil
			}
		}
		return false, nil
	})
	return waitErr
}

// getRestConfigClient returns  rest config client.
func getRestConfigClient() *rest.Config {
	// Get restConfig.
	var err error
	if restConfig == nil {
		if supervisorCluster || vanillaCluster {
			k8senv := GetAndExpectStringEnvVar("KUBECONFIG")
			restConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
		}
		if guestCluster {
			if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
				restConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
			}
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}
	return restConfig
}

// GetResizedStatefulSetFromManifest returns a StatefulSet from a manifest
// stored in fileName by adding namespace and a newSize.
func GetResizedStatefulSetFromManifest(ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(manifestPath, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	ss, err = statefulSetFromManifest(ssManifestFilePath, ss)
	framework.ExpectNoError(err)
	return ss
}

// statefulSetFromManifest returns a StatefulSet from a manifest stored in
// fileName in the Namespace indicated by ns.
func statefulSetFromManifest(fileName string, ss *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	currentSize := ss.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage()
	newSize := currentSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	ss.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[v1.ResourceStorage] = newSize

	return ss, nil
}

// collectPodLogs collects logs from all the pods in the namespace
func collectPodLogs(ctx context.Context, client clientset.Interface, namespace string) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	newpath := filepath.Join(".", "logs")
	err = os.MkdirAll(newpath, os.ModePerm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, pod := range pods.Items {
		framework.Logf("Collecting log from pod %v", pod.Name)
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val := strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring := strconv.FormatInt(curtime, 10)
		filename := curtimestring + val

		//Collect Pod logs
		for _, cont := range pod.Spec.Containers {
			output, err := fpod.GetPodLogs(client, pod.Namespace, pod.Name, cont.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Writing the logs into the file %v", "logs/"+pod.Name+cont.Name+filename)
			err = writeToFile("logs/"+pod.Name+cont.Name+filename, output)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
}

/*
This method works for 2 level topology enabled testbeds.
verifyPVnodeAffinityAndPODnodedetailsForStatefulsets verifies that PV node Affinity rules
should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on
which PV is provisioned.
*/
func verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx context.Context,
	client clientset.Interface, statefulset *appsv1.StatefulSet,
	namespace string, zoneValues []string, regionValues []string) {
	ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				// verify pv node affinity details
				pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				nodeList, err := fnodes.GetReadySchedulableNodes(client)
				framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
				if !(len(nodeList.Items) > 0) {
					framework.Failf("Unable to find ready and schedulable Node")
				}
				//verify node topology details
				err = verifyPodLocation(&sspod, nodeList, pvZone, pvRegion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Verify the attached volume match the one in CNS cache
				error := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
				gomega.Expect(error).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

// isCsiFssEnabled checks if the given CSI FSS is enabled or not, errors out if not found
func isCsiFssEnabled(ctx context.Context, client clientset.Interface, namespace string, fss string) bool {
	fssCM, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, csiFssCM, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	fssFound := false
	for k, v := range fssCM.Data {
		if fss == k {
			fssFound = true
			if v == "true" {
				return true
			}
		}
	}
	if !fssFound {
		framework.Logf("FSS %s not found in the %s configmap in namespace %s", fss, csiFssCM, namespace)
	}
	return false
}

/*
This wrapper method is used to create the topology map of allowed topologies specified on VC.
TOPOLOGY_MAP = "region:region1;zone:zone1;building:building1;level:level1;rack:rack1,rack2,rack3"
*/
func createTopologyMapLevel5(topologyMapStr string, level int) (map[string][]string, []string) {
	topologyMap := make(map[string][]string)
	var categories []string
	topologyFeature := os.Getenv(topologyFeature)

	if level != 5 && topologyFeature != topologyTkgHaName {
		return nil, categories
	}
	topologyCategories := strings.Split(topologyMapStr, ";")
	for _, category := range topologyCategories {
		categoryVal := strings.Split(category, ":")
		key := categoryVal[0]
		categories = append(categories, key)
		values := strings.Split(categoryVal[1], ",")
		topologyMap[key] = values
	}
	return topologyMap, categories
}

/*
This wrapper method is used to create allowed topologies set required for creating Storage Class.
*/
func createAllowedTopolgies(topologyMapStr string, level int) []v1.TopologySelectorLabelRequirement {
	topologyFeature := os.Getenv(topologyFeature)
	topologyMap, _ := createTopologyMapLevel5(topologyMapStr, level)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{}
	topoKey := ""
	if topologyFeature == topologyTkgHaName {
		topoKey = tkgHATopologyKey
	} else {
		topoKey = topologykey
	}
	for key, val := range topologyMap {
		allowedTopology := v1.TopologySelectorLabelRequirement{
			Key:    topoKey + "/" + key,
			Values: val,
		}
		allowedTopologies = append(allowedTopologies, allowedTopology)
	}
	return allowedTopologies
}

/*
This is a wrapper method which is used to create a topology map of all tags and categoties.
*/
func createAllowedTopologiesMap(allowedTopologies []v1.TopologySelectorLabelRequirement) map[string][]string {
	allowedTopologiesMap := make(map[string][]string)
	for _, topologySelector := range allowedTopologies {
		allowedTopologiesMap[topologySelector.Key] = topologySelector.Values
	}
	return allowedTopologiesMap
}

// GetPodList gets the current Pods in ss.
func GetListOfPodsInSts(c clientset.Interface, ss *appsv1.StatefulSet) *v1.PodList {
	selector, err := metav1.LabelSelectorAsSelector(ss.Spec.Selector)
	framework.ExpectNoError(err)
	var StatefulSetPods *v1.PodList = new(v1.PodList)
	podList, err := c.CoreV1().Pods(ss.Namespace).List(context.TODO(),
		metav1.ListOptions{LabelSelector: selector.String()})
	framework.ExpectNoError(err)
	for _, sspod := range podList.Items {
		if strings.Contains(sspod.Name, ss.Name) {
			StatefulSetPods.Items = append(StatefulSetPods.Items, sspod)
		}
	}
	return StatefulSetPods
}

/*
verifyVolumeTopologyForLevel5 verifies that the pv node affinity details should match the
allowed topologies specified in the storage class.
This method returns true if allowed topologies of SC matches with the PV node
affinity details else return error and false.
*/
func verifyVolumeTopologyForLevel5(pv *v1.PersistentVolume, allowedTopologiesMap map[string][]string) (bool, error) {
	if pv.Spec.NodeAffinity == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return false, fmt.Errorf("node Affinity rules for PV should exist in topology aware provisioning")
	}
	topologyFeature := os.Getenv(topologyFeature)
	for _, nodeSelector := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, topology := range nodeSelector.MatchExpressions {
			if val, ok := allowedTopologiesMap[topology.Key]; ok {
				if !compareStringLists(val, topology.Values) {
					if topologyFeature == topologyTkgHaName {
						return false, fmt.Errorf("pv node affinity details: %v does not match"+
							"with: %v in the allowed topologies", topology.Values, val)
					} else {
						return false, fmt.Errorf("PV node affinity details does not exist in the allowed " +
							"topologies specified in SC")
					}
				}
			} else {
				if topologyFeature == topologyTkgHaName {
					return false, fmt.Errorf("pv node affinity key: %v does not does not exist in the"+
						"allowed topologies map: %v", topology.Key, allowedTopologiesMap)
				} else {
					return false, fmt.Errorf("PV node affinity details does not exist in the allowed " +
						"topologies specified in SC")
				}
			}
		}
	}
	return true, nil
}

/*
This is a wrapper method which is used to compare 2 string list and returns true if value matches else returns false.
*/
func compareStringLists(strList1 []string, strList2 []string) bool {
	strMap := make(map[string]bool)
	for _, str := range strList1 {
		strMap[str] = true
	}
	for _, str := range strList2 {
		if _, ok := strMap[str]; !ok {
			return false
		}
	}
	return true
}

/*
For Statefulset Pod
verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5 for Statefulset verifies that PV
node Affinity rules should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on
which PV is provisioned.
*/
func verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx context.Context,
	client clientset.Interface, statefulset *appsv1.StatefulSet, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, parallelStatefulSetCreation bool) {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	var ssPodsBeforeScaleDown *v1.PodList
	if parallelStatefulSetCreation {
		ssPodsBeforeScaleDown = GetListOfPodsInSts(client, statefulset)
	} else {
		ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset)
	}
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				// get pv details
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

				// verify pv node affinity details as specified on SC
				ginkgo.By("Verifying PV node affinity details")
				res, err := verifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
				if res {
					framework.Logf("PV %s node affinity details lies in the specified allowed topologies of Storage Class", pv.Name)
				}
				gomega.Expect(res).To(gomega.BeTrue(), "PV %s node affinity details is not in the "+
					"specified allowed topologies of Storage Class", pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// fetch node details
				nodeList, err := fnodes.GetReadySchedulableNodes(client)
				framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
				if !(len(nodeList.Items) > 0) {
					framework.Failf("Unable to find ready and schedulable Node")
				}
				// verify pod is running on appropriate nodes
				ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
				res, err = verifyPodLocationLevel5(&sspod, nodeList, allowedTopologiesMap)
				if res {
					framework.Logf("Pod %v is running on appropriate node as specified "+
						"in the allowed topolgies of Storage Class", sspod.Name)
				}
				gomega.Expect(res).To(gomega.BeTrue(), "Pod %v is not running on appropriate node "+
					"as specified in allowed topolgies of Storage Class", sspod.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify the attached volume match the one in CNS cache
				error := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
				gomega.Expect(error).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

/*
verifyPodLocationLevel5 verifies that a pod is scheduled on a node that belongs to the
topology on which PV is provisioned.
This method returns true if all topology labels matches else returns false and error.
*/
func verifyPodLocationLevel5(pod *v1.Pod, nodeList *v1.NodeList,
	allowedTopologiesMap map[string][]string) (bool, error) {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			for labelKey, labelValue := range node.Labels {
				if topologyValue, ok := allowedTopologiesMap[labelKey]; ok {
					if !contains(topologyValue, labelValue) {
						return false, fmt.Errorf("pod: %s is not running on node located in %s", pod.Name, labelValue)
					}
				}
			}
		}
	}
	return true, nil
}

// udpate updates a statefulset, and it is only used within rest.go
func updateSts(c clientset.Interface, ns, name string, update func(ss *appsv1.StatefulSet)) *appsv1.StatefulSet {
	for i := 0; i < 3; i++ {
		ss, err := c.AppsV1().StatefulSets(ns).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			framework.Failf("failed to get statefulset %q: %v", name, err)
		}
		update(ss)
		ss, err = c.AppsV1().StatefulSets(ns).Update(context.TODO(), ss, metav1.UpdateOptions{})
		if err == nil {
			return ss
		}
		if !apierrors.IsConflict(err) && !apierrors.IsServerTimeout(err) {
			framework.Failf("failed to update statefulset %q: %v", name, err)
		}
	}
	framework.Failf("too many retries draining statefulset %q", name)
	return nil
}

// Scale scales ss to count replicas.
func scaleStatefulSetPods(c clientset.Interface, ss *appsv1.StatefulSet, count int32) (*appsv1.StatefulSet, error) {
	name := ss.Name
	ns := ss.Namespace
	StatefulSetPoll := 10 * time.Second
	StatefulSetTimeout := 10 * time.Minute
	framework.Logf("Scaling statefulset %s to %d", name, count)
	ss = updateSts(c, ns, name, func(ss *appsv1.StatefulSet) { *(ss.Spec.Replicas) = count })

	var statefulPodList *v1.PodList
	pollErr := wait.PollImmediate(StatefulSetPoll, StatefulSetTimeout, func() (bool, error) {
		statefulPodList = GetListOfPodsInSts(c, ss)
		if int32(len(statefulPodList.Items)) == count {
			return true, nil
		}
		return false, nil
	})
	if pollErr != nil {
		unhealthy := []string{}
		for _, statefulPod := range statefulPodList.Items {
			delTs, phase, readiness := statefulPod.DeletionTimestamp, statefulPod.Status.Phase,
				podutils.IsPodReady(&statefulPod)
			if delTs != nil || phase != v1.PodRunning || !readiness {
				unhealthy = append(unhealthy, fmt.Sprintf("%v: deletion %v, phase %v, readiness %v",
					statefulPod.Name, delTs, phase, readiness))
			}
		}
		return ss, fmt.Errorf("failed to scale statefulset to %d in %v. "+
			"Remaining pods:\n%v", count, StatefulSetTimeout, unhealthy)
	}
	return ss, nil
}

/*
scaleDownStatefulSetPod is a utility method which is used to scale down the count of StatefulSet replicas.
*/
func scaleDownStatefulSetPod(ctx context.Context, client clientset.Interface,
	statefulset *appsv1.StatefulSet, namespace string, replicas int32, parallelStatefulSetCreation bool) {
	ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
	var ssPodsAfterScaleDown *v1.PodList
	if parallelStatefulSetCreation {
		_, scaledownErr := scaleStatefulSetPods(client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulset)
	} else {
		_, scaledownErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		ssPodsAfterScaleDown = fss.GetPodList(client, statefulset)
	}

	// After scale down, verify vSphere volumes are detached from deleted pods
	ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
	for _, sspod := range ssPodsAfterScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
						client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume %q is not detached from the node %q",
							pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
				}
			}
		}
	}
	// After scale down, verify the attached volumes match those in CNS Cache
	for _, sspod := range ssPodsAfterScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

/*
scaleUpStatefulSetPod is a utility method which is used to scale up the count of StatefulSet replicas.
*/
func scaleUpStatefulSetPod(ctx context.Context, client clientset.Interface,
	statefulset *appsv1.StatefulSet, namespace string, replicas int32, parallelStatefulSetCreation bool) {
	ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
	var ssPodsAfterScaleUp *v1.PodList
	if parallelStatefulSetCreation {
		_, scaleupErr := scaleStatefulSetPods(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		ssPodsAfterScaleUp = GetListOfPodsInSts(client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	} else {
		_, scaleupErr := fss.Scale(client, statefulset, replicas)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)

		ssPodsAfterScaleUp = fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	}

	// After scale up, verify all vSphere volumes are attached to node VMs.
	ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
	for _, sspod := range ssPodsAfterScaleUp.Items {
		err := fpod.WaitTimeoutForPodReadyInNamespace(client, sspod.Name, statefulset.Namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
				var vmUUID string
				var exists bool
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if vanillaCluster {
					vmUUID = getNodeUUID(ctx, client, sspod.Spec.NodeName)
				} else {
					annotations := pod.Annotations
					vmUUID, exists = annotations[vmUUIDLabel]
					gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
					_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
				ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
				err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

/*
This wrapper method is used to fetch allowed topologies from default topology set
required for creating Storage Class specific to testcase scenarios.
*/
func getTopologySelector(topologyAffinityDetails map[string][]string,
	topologyCategories []string, level int,
	position ...int) []v1.TopologySelectorLabelRequirement {
	topologyFeature := os.Getenv(topologyFeature)
	var key string
	if topologyFeature == topologyTkgHaName {
		key = tkgHATopologyKey
	} else {
		key = topologykey
	}
	allowedTopologyForSC := []v1.TopologySelectorLabelRequirement{}
	updateLvl := -1
	var rnges []int
	if len(position) > 0 {
		updateLvl = position[0]
		rnges = position[1:]
	}
	for i := 0; i < level; i++ {
		var values []string
		category := topologyCategories[i]
		if i == updateLvl {
			for _, rng := range rnges {
				values = append(values, topologyAffinityDetails[category][rng])
			}
		} else {
			if topologyFeature == topologyTkgHaName {
				values = topologyAffinityDetails[key+"/"+category]
			} else {
				values = topologyAffinityDetails[category]
			}
		}

		topologySelector := v1.TopologySelectorLabelRequirement{
			Key:    key + "/" + category,
			Values: values,
		}
		allowedTopologyForSC = append(allowedTopologyForSC, topologySelector)
	}
	return allowedTopologyForSC
}

// GetPodsForDeployment gets pods for the given deployment
func GetPodsForMultipleDeployment(client clientset.Interface, deployment *appsv1.Deployment) (*v1.PodList, error) {
	replicaSetSelector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}
	replicaSetListOptions := metav1.ListOptions{LabelSelector: replicaSetSelector.String()}
	allReplicaSets, err := client.AppsV1().ReplicaSets(deployment.Namespace).List(context.TODO(), replicaSetListOptions)
	if err != nil {
		return nil, err
	}
	ownedReplicaSets := make([]*appsv1.ReplicaSet, 0, len(allReplicaSets.Items))
	for _, rs := range allReplicaSets.Items {
		if !metav1.IsControlledBy(&rs, deployment) {
			continue
		}
		if strings.Contains(rs.Name, deployment.Name) {
			ownedReplicaSets = append(ownedReplicaSets, &rs)
		}
	}
	var replicaSet *appsv1.ReplicaSet
	sort.Sort(replicaSetsByCreationTimestampDate(ownedReplicaSets))
	for _, rs := range ownedReplicaSets {
		replicaSet = rs
	}

	if replicaSet == nil {
		return nil, fmt.Errorf("expected a new replica set for deployment %q, found none", deployment.Name)
	}

	podSelector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}
	podListOptions := metav1.ListOptions{LabelSelector: podSelector.String()}
	allPods, err := client.CoreV1().Pods(deployment.Namespace).List(context.TODO(), podListOptions)
	if err != nil {
		return nil, err
	}
	ownedPods := &v1.PodList{Items: make([]v1.Pod, 0, len(allPods.Items))}
	for _, pod := range allPods.Items {
		if strings.Contains(pod.Name, deployment.Name) {
			ownedPods.Items = append(ownedPods.Items, pod)
		}
	}
	return ownedPods, nil
}

/*
For Deployment Pod
verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5 for Deployment verifies that PV node
Affinity rules should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on which
PV is provisioned.
*/
func verifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx context.Context,
	client clientset.Interface, deployment *appsv1.Deployment, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, parallelDeplCreation bool) {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	var pods *v1.PodList
	var err error
	if parallelDeplCreation {
		pods, err = GetPodsForMultipleDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		pods, err = fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	for _, sspod := range pods.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				// get pv details
				pv := getPvFromClaim(client, deployment.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

				// verify pv node affinity details as specified on SC
				ginkgo.By("Verifying PV node affinity details")
				res, err := verifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
				if res {
					framework.Logf("PV %s node affinity details lies in the specified allowed topologies of Storage Class", pv.Name)
				}
				gomega.Expect(res).To(gomega.BeTrue(), "PV %s node affinity details is not in the "+
					"specified allowed topologies of Storage Class", pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// fetch node details
				nodeList, err := fnodes.GetReadySchedulableNodes(client)
				framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
				if !(len(nodeList.Items) > 0) {
					framework.Failf("Unable to find ready and schedulable Node")
				}
				// verify pod is running on appropriate nodes
				ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
				res, err = verifyPodLocationLevel5(&sspod, nodeList, allowedTopologiesMap)
				if res {
					framework.Logf("Pod %v is running on appropriate node as specified in the "+
						"allowed topolgies of Storage Class", sspod.Name)
				}
				gomega.Expect(res).To(gomega.BeTrue(), "Pod %v is not running on appropriate node "+
					"as specified in allowed topolgies of Storage Class", sspod.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify the attached volume match the one in CNS cache
				error := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
				gomega.Expect(error).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

/*
For Standalone Pod
verifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5 for Standalone Pod verifies that PV
node Affinity rules should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on which PV
is provisioned.
*/
func verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5(ctx context.Context,
	client clientset.Interface, pod *v1.Pod, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement) {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	for _, volumespec := range pod.Spec.Volumes {
		if volumespec.PersistentVolumeClaim != nil {
			// get pv details
			pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

			// verify pv node affinity details as specified on SC
			ginkgo.By("Verifying PV node affinity details")
			res, err := verifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
			if res {
				framework.Logf("PV %s node affinity details lies in the specified allowed topologies of Storage Class", pv.Name)
			}
			gomega.Expect(res).To(gomega.BeTrue(), "PV %s node affinity details is not in the specified "+
				"allowed topologies of Storage Class", pv.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// fetch node details
			nodeList, err := fnodes.GetReadySchedulableNodes(client)
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
			// verify pod is running on appropriate nodes
			ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
			res, err = verifyPodLocationLevel5(pod, nodeList, allowedTopologiesMap)
			if res {
				framework.Logf("Pod %v is running on appropriate node as specified in the allowed "+
					"topolgies of Storage Class", pod.Name)
			}
			gomega.Expect(res).To(gomega.BeTrue(), "Pod %v is not running on appropriate node as "+
				"specified in allowed topolgies of Storage Class", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify the attached volume match the one in CNS cache
			error := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
				volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
			gomega.Expect(error).NotTo(gomega.HaveOccurred())
		}
	}
}

func getPersistentVolumeSpecWithStorageClassFCDNodeSelector(volumeHandle string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, storageClass string,
	labels map[string]string, sizeOfDisk string,
	allowedTopologies []v1.TopologySelectorLabelRequirement) *v1.PersistentVolume {
	var (
		pvConfig fpv.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = fpv.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIDriverName,
				VolumeHandle: volumeHandle,
				ReadOnly:     false,
				FSType:       "ext4",
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse(sizeOfDisk),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:         claimRef,
			StorageClassName: storageClass,
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIDriverName
	pv.Annotations = annotations
	pv.Spec.NodeAffinity = new(v1.VolumeNodeAffinity)
	pv.Spec.NodeAffinity.Required = new(v1.NodeSelector)
	pv.Spec.NodeAffinity.Required.NodeSelectorTerms = getNodeSelectorTerms(allowedTopologies)
	return pv
}

func getNodeSelectorTerms(allowedTopologies []v1.TopologySelectorLabelRequirement) []v1.NodeSelectorTerm {
	var nodeSelectorRequirements []v1.NodeSelectorRequirement
	var nodeSelectorTerms []v1.NodeSelectorTerm

	for i := 0; i < len(allowedTopologies)-1; i++ {
		topologySelector := allowedTopologies[i]
		var nodeSelectorRequirement v1.NodeSelectorRequirement
		nodeSelectorRequirement.Key = topologySelector.Key
		nodeSelectorRequirement.Operator = "In"
		nodeSelectorRequirement.Values = topologySelector.Values
		nodeSelectorRequirements = append(nodeSelectorRequirements, nodeSelectorRequirement)
	}
	rackTopology := allowedTopologies[len(allowedTopologies)-1]
	for i := 0; i < len(rackTopology.Values); i++ {
		var nodeSelectorTerm v1.NodeSelectorTerm
		var nodeSelectorRequirement v1.NodeSelectorRequirement
		nodeSelectorRequirement.Key = rackTopology.Key
		nodeSelectorRequirement.Operator = "In"
		nodeSelectorRequirement.Values = append(nodeSelectorRequirement.Values, rackTopology.Values[i])
		nodeSelectorTerm.MatchExpressions = append(nodeSelectorRequirements, nodeSelectorRequirement)
		nodeSelectorTerms = append(nodeSelectorTerms, nodeSelectorTerm)
	}
	return nodeSelectorTerms
}

// replicaSetsByCreationTimestamp sorts a list of ReplicaSet by creation timestamp, using their names as a tie breaker.
type replicaSetsByCreationTimestampDate []*appsv1.ReplicaSet

func (o replicaSetsByCreationTimestampDate) Len() int      { return len(o) }
func (o replicaSetsByCreationTimestampDate) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o replicaSetsByCreationTimestampDate) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// createKubernetesClientFromConfig creaates a newk8s client from given
// kubeConfig file.
func createKubernetesClientFromConfig(kubeConfigPath string) (clientset.Interface, error) {

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, err
	}

	client, err := clientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// getVolumeSnapshotClassSpec returns a spec for the volume snapshot class
func getVolumeSnapshotClassSpec(deletionPolicy snapc.DeletionPolicy,
	parameters map[string]string) *snapc.VolumeSnapshotClass {
	var volumesnapshotclass = &snapc.VolumeSnapshotClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshotClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "volumesnapshot-",
		},
		Driver:         e2evSphereCSIDriverName,
		DeletionPolicy: deletionPolicy,
	}

	volumesnapshotclass.Parameters = parameters
	return volumesnapshotclass
}

// getVolumeSnapshotSpec returns a spec for the volume snapshot
func getVolumeSnapshotSpec(namespace string, snapshotclassname string, pvcName string) *snapc.VolumeSnapshot {
	var volumesnapshotSpec = &snapc.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "snapshot-",
			Namespace:    namespace,
		},
		Spec: snapc.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &snapshotclassname,
			Source: snapc.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	return volumesnapshotSpec
}

// waitForVolumeSnapshotReadyToUse waits for the volume's snapshot to be in ReadyToUse
func waitForVolumeSnapshotReadyToUse(client snapclient.Clientset, ctx context.Context, namespace string,
	name string) (*snapc.VolumeSnapshot, error) {
	var volumeSnapshot *snapc.VolumeSnapshot
	var err error
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		volumeSnapshot, err = client.SnapshotV1().VolumeSnapshots(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("error fetching volumesnapshot details : %v", err)
		}
		if volumeSnapshot.Status != nil && *volumeSnapshot.Status.ReadyToUse {
			return true, nil
		}
		return false, nil
	})
	return volumeSnapshot, waitErr
}

// waitForVolumeSnapshotContentToBeDeleted wait till the volume snapshot content is deleted
func waitForVolumeSnapshotContentToBeDeleted(client snapclient.Clientset, ctx context.Context,
	name string) error {
	var err error
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		_, err = client.SnapshotV1().VolumeSnapshotContents().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return true, nil
			} else {
				return false, fmt.Errorf("error fetching volumesnapshotcontent details : %v", err)
			}
		}
		return false, nil
	})
	return waitErr
}

// getK8sMasterNodeIPWhereControllerLeaderIsRunning fetches the master node IP
// where controller is running
func getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx context.Context,
	client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	containerName string) (string, string, error) {
	ignoreLabels := make(map[string]string)
	csiControllerPodName, grepCmdForFindingCurrentLeader := "", ""
	csiPods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var k8sMasterIP, kubeConfigPath string
	if guestCluster {
		k8sMasterIP = GetAndExpectStringEnvVar(svcMasterIP)
		kubeConfigPath = GetAndExpectStringEnvVar(gcKubeConfigPath)
	} else {
		k8sMasterIPs := getK8sMasterIPs(ctx, client)
		k8sMasterIP = k8sMasterIPs[0]
	}
	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, vSphereCSIControllerPodNamePrefix) {
			// Putting the grepped logs for leader of container of different CSI pods
			// to same temporary file
			// NOTE: This is not valid for vsphere-csi-controller container as for
			// vsphere-csi-controller all the replicas will behave as leaders
			grepCmdForFindingCurrentLeader = "echo `kubectl logs " + csiPod.Name + " -n " +
				csiSystemNamespace + " " + containerName + " | grep 'successfully acquired lease' | " +
				"tail -1` 'podName:" + csiPod.Name + "' | tee -a leader.log"
			if guestCluster {
				grepCmdForFindingCurrentLeader = fmt.Sprintf("echo `kubectl logs "+csiPod.Name+" -n "+
					csiSystemNamespace+" "+containerName+" --kubeconfig %s "+" | grep 'successfully acquired lease' | "+
					"tail -1` 'podName:"+csiPod.Name+"' | tee -a leader.log", kubeConfigPath)
			}

			framework.Logf("Invoking command '%v' on host %v", grepCmdForFindingCurrentLeader,
				k8sMasterIP)
			result, err := sshExec(sshClientConfig, k8sMasterIP,
				grepCmdForFindingCurrentLeader)
			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
					grepCmdForFindingCurrentLeader, k8sMasterIP, err)
			}
		}
	}

	// Sorting the temporary file according to timestamp to find the latest container leader
	// from the CSI pod replicas
	var cmd string

	cmd = "sort -k 2n leader.log | tail -1 | sed -n 's/.*podName://p' | tr -d '\n'"

	framework.Logf("Invoking command '%v' on host %v", cmd,
		k8sMasterIP)
	result, err := sshExec(sshClientConfig, k8sMasterIP,
		cmd)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, k8sMasterIP, err)
	}
	csiControllerPodName = result.Stdout

	// delete the temporary log file
	cmd = "rm leader.log"
	framework.Logf("Invoking command '%v' on host %v", cmd,
		k8sMasterIP)
	result, err = sshExec(sshClientConfig, k8sMasterIP,
		cmd)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, k8sMasterIP, err)
	}

	if csiControllerPodName == "" {
		return "", "", fmt.Errorf("couldn't find CSI pod where %s leader is running",
			containerName)
	}

	framework.Logf("CSI pod %s where %s leader is running", csiControllerPodName, containerName)
	// Fetching master node name where container leader is running
	podData, err := client.CoreV1().Pods(csiSystemNamespace).Get(ctx, csiControllerPodName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	masterNodeName := podData.Spec.NodeName
	framework.Logf("Master node name %s where %s leader is running", masterNodeName, containerName)
	// Fetching IP address of master node where container leader is running
	k8sMasterNodeIP, err := getMasterIpFromMasterNodeName(ctx, client, masterNodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Master node ip %s where %s leader is running", k8sMasterNodeIP, containerName)
	return csiControllerPodName, k8sMasterNodeIP, nil
}

// execDockerPauseNKillOnContainer pauses and then kills the particular CSI container on given master node
func execDockerPauseNKillOnContainer(sshClientConfig *ssh.ClientConfig, k8sMasterNodeIP string,
	containerName string, k8sVersion string) error {
	containerPauseCmd := ""
	containerKillCmd := ""
	k8sVer, err := strconv.ParseFloat(k8sVersion, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	containerID, err := waitAndGetContainerID(sshClientConfig, k8sMasterNodeIP, containerName, k8sVer)
	if err != nil {
		return err
	}
	if k8sVer <= 1.23 {
		containerPauseCmd = "docker pause " + containerID
	} else {
		containerPauseCmd = "nerdctl pause --namespace k8s.io " + containerID
	}
	framework.Logf("Invoking command '%v' on host %v", containerPauseCmd,
		k8sMasterNodeIP)
	cmdResult, err := sshExec(sshClientConfig, k8sMasterNodeIP,
		containerPauseCmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			containerPauseCmd, k8sMasterNodeIP, err)
	}
	framework.Logf("Waiting for 5 seconds as leader election happens every 5 secs")
	time.Sleep(5 * time.Second)
	if k8sVer <= 1.23 {
		containerKillCmd = "docker kill " + containerID
	} else {
		containerKillCmd = "nerdctl kill --namespace k8s.io " + containerID
	}
	framework.Logf("Invoking command '%v' on host %v", containerKillCmd,
		k8sMasterNodeIP)
	cmdResult, err = sshExec(sshClientConfig, k8sMasterNodeIP,
		containerKillCmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		if strings.Contains(cmdResult.Stderr, "OCI runtime resume failed") {
			return nil
		}
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			containerKillCmd, k8sMasterNodeIP, err)
	}
	return nil
}

// Fetching IP address of master node from a given master node name
func getMasterIpFromMasterNodeName(ctx context.Context, client clientset.Interface,
	masterNodeName string) (string, error) {
	k8sMasterNodeIP := ""
	k8sNodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, node := range k8sNodes.Items {
		if node.Name == masterNodeName {
			addrs := node.Status.Addresses
			for _, addr := range addrs {
				if addr.Type == v1.NodeInternalIP && (net.ParseIP(addr.Address)).To4() != nil {
					k8sMasterNodeIP = addr.Address
					break
				}
			}
		}
	}
	if k8sMasterNodeIP != "" {
		return k8sMasterNodeIP, nil
	} else {
		return "", fmt.Errorf("couldn't find master ip from master node: %s", masterNodeName)
	}
}

// getVolumeSnapshotContentSpec returns a spec for the volume snapshot content
func getVolumeSnapshotContentSpec(deletionPolicy snapc.DeletionPolicy, snapshotHandle string,
	futureSnapshotName string, namespace string) *snapc.VolumeSnapshotContent {
	var volumesnapshotContentSpec = &snapc.VolumeSnapshotContent{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshotContent",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "snapshotcontent-",
		},
		Spec: snapc.VolumeSnapshotContentSpec{
			DeletionPolicy: deletionPolicy,
			Driver:         e2evSphereCSIDriverName,
			Source: snapc.VolumeSnapshotContentSource{
				SnapshotHandle: &snapshotHandle,
			},
			VolumeSnapshotRef: v1.ObjectReference{
				Name:      futureSnapshotName,
				Namespace: namespace,
			},
		},
	}
	return volumesnapshotContentSpec
}

// getVolumeSnapshotSpecByName returns a spec for the volume snapshot by name
func getVolumeSnapshotSpecByName(namespace string, snapshotName string,
	snapshotcontentname string) *snapc.VolumeSnapshot {
	var volumesnapshotSpec = &snapc.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: namespace,
		},
		Spec: snapc.VolumeSnapshotSpec{
			Source: snapc.VolumeSnapshotSource{
				VolumeSnapshotContentName: &snapshotcontentname,
			},
		},
	}
	return volumesnapshotSpec
}

func createParallelStatefulSets(client clientset.Interface, namespace string,
	statefulset *appsv1.StatefulSet, replicas int32, wg *sync.WaitGroup) {
	defer wg.Done()
	ginkgo.By("Creating statefulset")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf(fmt.Sprintf("Creating statefulset %v/%v with %d replicas and selector %+v",
		statefulset.Namespace, statefulset.Name, replicas, statefulset.Spec.Selector))
	_, err := client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func createParallelStatefulSetSpec(namespace string, no_of_sts int, replicas int32) []*appsv1.StatefulSet {
	stss := []*appsv1.StatefulSet{}
	var statefulset *appsv1.StatefulSet

	for i := 0; i < no_of_sts; i++ {
		statefulset = GetStatefulSetFromManifest(namespace)
		statefulset.Name = "thread-" + strconv.Itoa(i) + "-" + statefulset.Name
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Annotations["volume.beta.kubernetes.io/storage-class"] = defaultNginxStorageClassName
		statefulset.Spec.Replicas = &replicas
		stss = append(stss, statefulset)
	}
	return stss
}

func createMultiplePVCsInParallel(ctx context.Context, client clientset.Interface, namespace string,
	storageclass *storagev1.StorageClass, count int) []*v1.PersistentVolumeClaim {
	var pvclaims []*v1.PersistentVolumeClaim
	for i := 0; i < count; i++ {
		pvclaim, err := createPVC(client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims = append(pvclaims, pvclaim)
	}
	return pvclaims
}

// CheckMount checks that the mount at mountPath is valid for all Pods in ss.
func CheckMountForStsPods(c clientset.Interface, ss *appsv1.StatefulSet, mountPath string) error {
	for _, cmd := range []string{
		// Print inode, size etc
		fmt.Sprintf("ls -idlh %v", mountPath),
		// Print subdirs
		fmt.Sprintf("find %v", mountPath),
		// Try writing
		fmt.Sprintf("touch %v", filepath.Join(mountPath, fmt.Sprintf("%v", time.Now().UnixNano()))),
	} {
		if err := ExecInStsPodsInNs(c, ss, cmd); err != nil {
			return fmt.Errorf("failed to execute %v, error: %v", cmd, err)
		}
	}
	return nil
}

/*
	ExecInStsPodsInNs executes cmd in all Pods in ss. If a error occurs it is returned and

cmd is not execute in any subsequent Pods.
*/
func ExecInStsPodsInNs(c clientset.Interface, ss *appsv1.StatefulSet, cmd string) error {
	podList := GetListOfPodsInSts(c, ss)
	StatefulSetPoll := 10 * time.Second
	StatefulPodTimeout := 5 * time.Minute
	for _, statefulPod := range podList.Items {
		stdout, err := framework.RunHostCmdWithRetries(statefulPod.Namespace,
			statefulPod.Name, cmd, StatefulSetPoll, StatefulPodTimeout)
		framework.Logf("stdout of %v on %v: %v", cmd, statefulPod.Name, stdout)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
This method is used to delete the CSI Controller Pod
*/
func deleteCsiControllerPodWhereLeaderIsRunning(ctx context.Context,
	client clientset.Interface, csi_controller_pod string) error {
	ignoreLabels := make(map[string]string)
	csiPods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	num_csi_pods := len(csiPods)
	// Collecting and dumping csi pod logs before deleting them
	collectPodLogs(ctx, client, csiSystemNamespace)
	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, vSphereCSIControllerPodNamePrefix) && csiPod.Name == csi_controller_pod {
			framework.Logf("Deleting the pod: %s", csiPod.Name)
			err = fpod.DeletePodWithWait(client, csiPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	// wait for csi Pods to be in running ready state
	err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0, pollTimeout, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return nil
}

// getPersistentVolumeClaimSpecWithDatasource return the PersistentVolumeClaim
// spec with specified storage class.
func getPersistentVolumeClaimSpecWithDatasource(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
	datasourceName string, snapshotapigroup string) *v1.PersistentVolumeClaim {
	disksize := diskSize
	if ds != "" {
		disksize = ds
	}
	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(disksize),
				},
			},
			StorageClassName: &(storageclass.Name),
			DataSource: &v1.TypedLocalObjectReference{
				APIGroup: &snapshotapigroup,
				Kind:     "VolumeSnapshot",
				Name:     datasourceName,
			},
		},
	}

	if pvclaimlabels != nil {
		claim.Labels = pvclaimlabels
	}

	return claim
}

// get topology cluster lists
func ListTopologyClusterNames(topologyCluster string) []string {
	topologyClusterList := strings.Split(topologyCluster, ",")
	return topologyClusterList
}

// getHosts returns list of hosts and it takes clusterComputeResource as input.
func getHostsByClusterName(ctx context.Context, clusterComputeResource []*object.ClusterComputeResource,
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

// This util method powers on esxi hosts which were powered off cluster wise
func powerOnEsxiHostByCluster(hostToPowerOn string) {
	var esxHostIp string = ""
	for _, esxInfo := range tbinfo.esxHosts {
		if hostToPowerOn == esxInfo["vmName"] {
			esxHostIp = esxInfo["ip"]
			err := vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, hostToPowerOn, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
	}
	err := waitForHostToBeUp(esxHostIp)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// This util method takes cluster name as input parameter and powers off esxi host of that cluster
func powerOffEsxiHostByCluster(ctx context.Context, vs *vSphere, clusterName string,
	esxCount int) []string {
	var powerOffHostsList []string
	var hostsInCluster []*object.HostSystem
	clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	hostsInCluster = getHostsByClusterName(ctx, clusterComputeResource, clusterName)
	for i := 0; i < esxCount; i++ {
		for _, esxInfo := range tbinfo.esxHosts {
			host := hostsInCluster[i].Common.InventoryPath
			hostIp := strings.Split(host, "/")
			if hostIp[len(hostIp)-1] == esxInfo["ip"] {
				esxHostName := esxInfo["vmName"]
				powerOffHostsList = append(powerOffHostsList, esxHostName)
				err = vMPowerMgmt(tbinfo.user, tbinfo.location, tbinfo.podname, esxHostName, false)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = waitForHostToBeDown(esxInfo["ip"])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	return powerOffHostsList
}

// getVolumeSnapshotSpecWithoutSC returns a spec for the volume snapshot
func getVolumeSnapshotSpecWithoutSC(namespace string, pvcName string) *snapc.VolumeSnapshot {
	var volumesnapshotSpec = &snapc.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			Kind: "VolumeSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "snapshot-",
			Namespace:    namespace,
		},
		Spec: snapc.VolumeSnapshotSpec{
			Source: snapc.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	return volumesnapshotSpec
}

// waitForPvcToBeDeleted waits by polling for a particular pvc to be deleted in a namespace
func waitForPvcToBeDeleted(ctx context.Context, client clientset.Interface, pvcName string, namespace string) error {
	var pvc *v1.PersistentVolumeClaim
	waitErr := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return true, nil
			} else {
				return false, fmt.Errorf("pvc %s is still not deleted in"+
					"namespace %s with err: %v", pvc.Name, namespace, err)
			}
		}
		return false, nil
	})
	framework.Logf("Status of pvc is: %v", pvc.Status.Phase)
	return waitErr
}

/*
	This util method fetches events list of the given object name and checkes for

specified error reason and returns true if expected error Reason found
*/
func waitForEventWithReason(client clientset.Interface, namespace string,
	name string, expectedErrMsg string) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	isFailureFound := false
	ginkgo.By("Checking for error in events related to pvc " + name)
	waitErr := wait.PollImmediate(poll, pollTimeoutShort, func() (bool, error) {
		eventList, _ := client.CoreV1().Events(namespace).List(ctx,
			metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", name)})
		for _, item := range eventList.Items {
			if strings.Contains(item.Reason, expectedErrMsg) {
				framework.Logf("Expected Error msg found. EventList Reason: "+
					"%q"+" EventList item: %q", item.Reason, item.Message)
				isFailureFound = true
				break
			}
		}
		return isFailureFound, nil
	})
	return isFailureFound, waitErr
}

// stopCSIPods function stops all the running csi pods
func stopCSIPods(ctx context.Context, client clientset.Interface) (bool, error) {
	collectPodLogs(ctx, client, csiSystemNamespace)
	isServiceStopped := false
	err := updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix,
		csiSystemNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = true
	return isServiceStopped, err
}

// startCSIPods function starts the csi pods and waits till all the pods comes up
func startCSIPods(ctx context.Context, client clientset.Interface, csiReplicas int32) (bool, error) {
	ignoreLabels := make(map[string]string)
	err := updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix,
		csiSystemNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Wait for the CSI Pods to be up and Running
	list_of_pods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	num_csi_pods := len(list_of_pods)
	err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(num_csi_pods), 0,
		pollTimeout, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped := false
	return isServiceStopped, err
}

// waitForStsPodsToBeInRunningState function waits till all the pods comes up
func waitForStsPodsToBeInReadyRunningState(ctx context.Context, client clientset.Interface, namespace string,
	statefulSets []*appsv1.StatefulSet) error {
	waitErr := wait.Poll(pollTimeoutShort, pollTimeoutShort*20, func() (bool, error) {
		for i := 0; i < len(statefulSets); i++ {
			fss.WaitForStatusReadyReplicas(client, statefulSets[i], *statefulSets[i].Spec.Replicas)
			pods := GetListOfPodsInSts(client, statefulSets[i])
			err := CheckMountForStsPods(client, statefulSets[i], mountPath)
			if err != nil {
				return false, err
			}
			if len(pods.Items) == 0 {
				return false, fmt.Errorf("unable to get list of Pods from the Statefulset: %v", statefulSets[i].Name)
			}
			if len(pods.Items) != int(*statefulSets[i].Spec.Replicas) {
				return false, fmt.Errorf("number of Pods in the statefulset should match with number of replicas")
			}
		}
		return true, nil
	})
	return waitErr
}

// enableFullSyncTriggerFss enables full sync fss in internal features configmap in csi namespace
func enableFullSyncTriggerFss(ctx context.Context, client clientset.Interface, namespace string, fss string) {
	fssCM, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, csiFssCM, metav1.GetOptions{})
	framework.Logf("%s configmap in namespace %s is %s", csiFssCM, namespace, fssCM)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	fssFound := false
	for k, v := range fssCM.Data {
		if fss == k && v != "true" {
			framework.Logf("FSS %s found in the %s configmap in namespace %s", fss, csiFssCM, namespace)
			fssFound = true
			fssCM.Data[fss] = "true"
			// Enable full sync fss by updating full sync field to true
			_, err = client.CoreV1().ConfigMaps(namespace).Update(ctx, fssCM, metav1.UpdateOptions{})
			framework.Logf("%s configmap in namespace %s is %s", csiFssCM, namespace, fssCM)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Collecting and dumping csi pod logs before killing them
			collectPodLogs(ctx, client, csiSystemNamespace)
			csipods, err := client.CoreV1().Pods(csiSystemNamespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, pod := range csipods.Items {
				fpod.DeletePodOrFail(client, csiSystemNamespace, pod.Name)
			}
			err = fpod.WaitForPodsRunningReady(client, csiSystemNamespace, int32(csipods.Size()), 0, pollTimeout, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			break
		} else if fss == k && v == "true" {
			framework.Logf("FSS %s found and is enabled in the %s configmap", fss, csiFssCM)
			fssFound = true
			break
		}
	}
	gomega.Expect(fssFound).To(gomega.BeTrue(),
		"FSS %s not found in the %s configmap in namespace %s", fss, csiFssCM, namespace)
}

// triggerFullSync triggers 2 full syncs on demand
func triggerFullSync(ctx context.Context, client clientset.Interface,
	cnsOperatorClient client.Client) {
	err := waitForFullSyncToFinish(client, ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
	crd := getTriggerFullSyncCrd(ctx, client, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)
	updateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd)
	err = waitForFullSyncToFinish(client, ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
	crd = getTriggerFullSyncCrd(ctx, client, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)
	updateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd)
	err = waitForFullSyncToFinish(client, ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
}

// waitAndGetContainerID waits and fetches containerID of a given containerName
func waitAndGetContainerID(sshClientConfig *ssh.ClientConfig, k8sMasterIP string,
	containerName string, k8sVersion float64) (string, error) {
	containerId := ""
	cmdToGetContainerId := ""
	waitErr := wait.PollImmediate(poll, pollTimeoutShort*3, func() (bool, error) {
		if k8sVersion <= 1.23 {
			cmdToGetContainerId = "docker ps | grep " + containerName + " | " +
				"awk '{print $1}' |  tr -d '\n'"
		} else {
			cmdToGetContainerId = "nerdctl --namespace k8s.io ps -a | grep -E " + containerName + ".*Up  | " +
				"awk '{print $1}' |  tr -d '\n'"
		}
		framework.Logf("Invoking command '%v' on host %v", cmdToGetContainerId,
			k8sMasterIP)
		dockerContainerInfo, err := sshExec(sshClientConfig, k8sMasterIP,
			cmdToGetContainerId)
		fssh.LogResult(dockerContainerInfo)
		containerId = dockerContainerInfo.Stdout
		if containerId != "" {
			return true, nil
		}
		if err != nil || dockerContainerInfo.Code != 0 {
			return false, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
				cmdToGetContainerId, k8sMasterIP, err)
		}
		return false, nil
	})

	if containerId == "" {
		return "", fmt.Errorf("couldn't get the containerId of :%s container", containerName)
	}
	return containerId, waitErr
}

// startVCServiceWait4VPs starts given service and waits for all VPs to come online
func startVCServiceWait4VPs(ctx context.Context, vcAddress string, service string, isSvcStopped *bool) {
	err := invokeVCenterServiceControl(startOperation, service, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = waitVCenterServiceToBeInState(service, vcAddress, svcRunningMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*isSvcStopped = false
}

// assignPolicyToWcpNamespace assigns a set of storage policies to a wcp namespace
func assignPolicyToWcpNamespace(client clientset.Interface, ctx context.Context,
	namespace string, policyNames []string) {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	vcAddress := vcIp + ":" + sshdPort
	sessionId := createVcSession4RestApis()

	curlStr := ""
	policyNamesArrLength := len(policyNames)
	defRqLimit := strings.Split(rqLimit, "Gi")[0]
	limit, err := strconv.Atoi(defRqLimit)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	limit *= 953 //to convert gb to mebibytes
	if policyNamesArrLength >= 1 {
		curlStr += fmt.Sprintf(`{ "limit": %d, "policy": "%s"}`, limit, e2eVSphere.GetSpbmPolicyID(policyNames[0]))
	}
	if policyNamesArrLength >= 2 {
		for i := 1; i < policyNamesArrLength; i++ {
			profileID := e2eVSphere.GetSpbmPolicyID(policyNames[i])
			curlStr += "," + fmt.Sprintf(`{ "limit": %d, "policy": "%s"}`, limit, profileID)
		}
	}

	httpCodeStr := `%{http_code}`
	curlCmd := fmt.Sprintf(`curl -s -o /dev/null -w "%s" -k -X PATCH`+
		` 'https://%s/api/vcenter/namespaces/instances/%s' -H `+
		`'vmware-api-session-id: %s' -H 'Content-type: application/json' -d `+
		`'{ "access_list": [ { "domain": "", "role": "OWNER", "subject": "", "subject_type": "USER" } ], `+
		`"description": "", "resource_spec": { }, "storage_specs": [ %s ], `+
		`"vm_service_spec": { } }'`, httpCodeStr, vcIp, namespace, sessionId, curlStr)

	framework.Logf("Running command: %s", curlCmd)
	result, err := fssh.SSH(curlCmd, vcAddress, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"couldn't execute command: %v due to err %v", curlCmd, err)
	}
	gomega.Expect(result.Stdout).To(gomega.Equal("204"))

	// wait for sc to get created in SVC
	for _, policyName := range policyNames {
		err = waitForScToGetCreated(client, ctx, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

}

// createVcSession4RestApis generates session ID for VC to use in rest API calls
func createVcSession4RestApis() string {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	vcAddress := vcIp + ":" + sshdPort
	nimbusGeneratedVcPwd := GetAndExpectStringEnvVar(vcUIPwd)
	curlCmd := fmt.Sprintf("curl -k -X POST https://%s/rest/com/vmware/cis/session"+
		" -u 'Administrator@vsphere.local:%s'", vcIp, nimbusGeneratedVcPwd)
	framework.Logf("Running command: %s", curlCmd)
	result, err := fssh.SSH(curlCmd, vcAddress, framework.TestContext.Provider)
	fssh.LogResult(result)
	if err != nil || result.Code != 0 {
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"couldn't execute command: %v due to err %v", curlCmd, err)
	}

	var session map[string]interface{}
	res := []byte(result.Stdout)
	err = json.Unmarshal(res, &session)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	sessionId := session["value"].(string)
	framework.Logf("sessionID is: %v", sessionId)
	return sessionId
}

// waitForScToGetCreated waits for a particular storageclass to get created
func waitForScToGetCreated(client clientset.Interface, ctx context.Context, policyName string) error {
	waitErr := wait.PollImmediate(poll, pollTimeoutShort*5, func() (bool, error) {
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("couldn't find storageclass: %s due to error: %v", policyName, err)
		}
		if storageclass != nil {
			return true, nil
		}
		return false, nil
	})
	if waitErr == wait.ErrWaitTimeout {
		return fmt.Errorf("couldn't find storageclass: %s in SVC", policyName)
	}
	return nil

}

// getHostIpWhereVmIsPresent uses govc command to fetch the host name where
// vm is present
func getHostIpWhereVmIsPresent(vmIp string) string {
	vcAddress := e2eVSphere.Config.Global.VCenterHostname
	dc := GetAndExpectStringEnvVar(datacenter)
	vcAdminPwd := GetAndExpectStringEnvVar(vcUIPwd)
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

// restartCSIDriver method restarts the csi driver
func scaleCSIDriver(ctx context.Context, client clientset.Interface, namespace string,
	csiReplicas int32) (bool, error) {
	err := updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix,
		csiSystemNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return true, nil
}

// getK8sMasterNodeIPWhereControllerLeaderIsRunning fetches the master node IP
// where controller is running
func getCSIPodWhereListVolumeResponseIsPresent(ctx context.Context,
	client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	containerName string, logMessage string, volumeids []string) (string, string, error) {
	ignoreLabels := make(map[string]string)
	csiControllerPodName, grepCmdForFindingCurrentLeader := "", ""
	csiPods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var k8sMasterIP string
	k8sMasterIPs := getK8sMasterIPs(ctx, client)
	k8sMasterIP = k8sMasterIPs[0]
	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, vSphereCSIControllerPodNamePrefix) {
			// Putting the grepped logs for leader of container of different CSI pods
			// to same temporary file
			// NOTE: This is not valid for vsphere-csi-controller container as for
			// vsphere-csi-controller all the replicas will behave as leaders
			grepCmdForFindingCurrentLeader = "echo `kubectl logs " + csiPod.Name + " -n " +
				csiSystemNamespace + " " + containerName + " | grep " + "'" + logMessage + "'| " +
				"tail -2` | tee -a listVolumeResponse.log"

			framework.Logf("Invoking command '%v' on host %v", grepCmdForFindingCurrentLeader,
				k8sMasterIP)
			result, err := sshExec(sshClientConfig, k8sMasterIP,
				grepCmdForFindingCurrentLeader)
			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
					grepCmdForFindingCurrentLeader, k8sMasterIP, err)
			} else {
				if len(volumeids) > 0 {
					fssh.LogResult(result)
					for i := 0; i < len(volumeids); i++ {
						framework.Logf("validating volumeID %s in response", volumeids[i])
						if !strings.Contains(result.Stdout, volumeids[i]) {
							return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
								grepCmdForFindingCurrentLeader, k8sMasterIP, err)
						}
					}
				}
			}

		}
	}

	// delete the temporary log file
	cmd := "rm listVolumeResponse.log"
	framework.Logf("Invoking command '%v' on host %v", cmd,
		k8sMasterIP)
	result, err := sshExec(sshClientConfig, k8sMasterIP, cmd)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, k8sMasterIP, err)
	}

	return csiControllerPodName, k8sMasterIP, nil
}

// Get all PVC list in the given namespace
func getAllPVCFromNamespace(client clientset.Interface, namespace string) *v1.PersistentVolumeClaimList {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvcList, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvcList).NotTo(gomega.BeNil())
	return pvcList
}
