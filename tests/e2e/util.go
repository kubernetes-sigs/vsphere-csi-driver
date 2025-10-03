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
	cryptoRand "crypto/rand"
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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	pkgtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/kubectl/pkg/drain"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	"k8s.io/kubernetes/test/e2e/framework/manifest"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	"k8s.io/pod-security-admission/api"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var (
	defaultCluster         *object.ClusterComputeResource
	svcClient              clientset.Interface
	svcNamespace           string
	vsanHealthClient       *VsanClient
	clusterComputeResource []*object.ClusterComputeResource
	defaultDatastore       *object.Datastore
	restConfig             *rest.Config
	pvclaims               []*v1.PersistentVolumeClaim
	pvclaimsToDelete       []*v1.PersistentVolumeClaim
	pvZone                 string
	pvRegion               string
	vmIp2MoMap             map[string]vim25types.ManagedObjectReference
	vcVersion              string
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

// This Struct is used for Creating tanzu cluster
type TanzuCluster struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		Topology struct {
			ControlPlane struct {
				TKR struct {
					Reference struct {
						Name string `yaml:"name"`
					} `yaml:"reference"`
				} `yaml:"tkr"`
				Replicas     int    `yaml:"replicas"`
				VMClass      string `yaml:"vmClass"`
				StorageClass string `yaml:"storageClass"`
			} `yaml:"controlPlane"`
			NodePools []struct {
				Replicas     int    `yaml:"replicas"`
				Name         string `yaml:"name"`
				VMClass      string `yaml:"vmClass"`
				StorageClass string `yaml:"storageClass"`
			} `yaml:"nodePools"`
		} `yaml:"topology"`
		Settings struct {
			Network struct {
				CNI struct {
					Name string `yaml:"name"`
				} `yaml:"cni"`
				Services struct {
					CIDRBlocks []string `yaml:"cidrBlocks"`
				} `yaml:"services"`
				Pods struct {
					CIDRBlocks []string `yaml:"cidrBlocks"`
				} `yaml:"pods"`
				ServiceDomain string `yaml:"serviceDomain"`
			} `yaml:"network"`
		} `yaml:"settings"`
	} `yaml:"spec"`
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

	adminClient, client := initializeClusterClientsByUserRoles(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv, err := adminClient.CoreV1().PersistentVolumes().Get(ctx, pvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pv
}

// getNodeUUID returns Node VM UUID for requested node.
func getNodeUUID(ctx context.Context, client clientset.Interface, nodeName string) string {
	vmUUID := ""
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
			Resources: v1.VolumeResourceRequirements{
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
			Resources: v1.VolumeResourceRequirements{
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
func createPVCAndStorageClass(ctx context.Context, client clientset.Interface, pvcnamespace string,
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

	pvclaim, err := createPVC(ctx, client, pvcnamespace, pvclaimlabels, ds, storageclass, accessMode)
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

	var err error
	adminClient, _ := initializeClusterClientsByUserRoles(client)
	var storageclass *storagev1.StorageClass
	isStorageClassPresent := false
	p := map[string]string{}
	if scParameters == nil && os.Getenv(envHciMountRemoteDs) == "true" {
		p[scParamStoragePolicyName] = os.Getenv(envStoragePolicyNameForHCIRemoteDatastores)
		scParameters = p
	}
	ginkgo.By(fmt.Sprintf("Creating StorageClass %s with scParameters: %+v and allowedTopologies: %+v "+
		"and ReclaimPolicy: %+v and allowVolumeExpansion: %t",
		scName, scParameters, allowedTopologies, scReclaimPolicy, allowVolumeExpansion))

	if supervisorCluster {
		storageclass, err = adminClient.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if storageclass != nil && err == nil {
			isStorageClassPresent = true
		}
	}

	if !isStorageClassPresent {
		storageclass, err = adminClient.StorageV1().StorageClasses().Create(ctx, getVSphereStorageClassSpec(scName,
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
func createPVC(ctx context.Context, client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string,
	ds string, storageclass *storagev1.StorageClass,
	accessMode v1.PersistentVolumeAccessMode) (*v1.PersistentVolumeClaim, error) {
	pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s "+
		"and labels: %+v accessMode: %+v", storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := fpv.CreatePVC(ctx, client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
	framework.Logf("PVC created: %v in namespace: %v", pvclaim.Name, pvcnamespace)
	return pvclaim, err
}

// createPVC helps creates pvc with given namespace and labels using given
// storage class.
func scaleCreatePVC(ctx context.Context, client clientset.Interface, pvcnamespace string,
	pvclaimlabels map[string]string, ds string, storageclass *storagev1.StorageClass,
	accessMode v1.PersistentVolumeAccessMode, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()

	pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and "+
		"labels: %+v accessMode: %+v", storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := fpv.CreatePVC(ctx, client, pvcnamespace, pvcspec)
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
	defer ginkgo.GinkgoRecover()
	ctx, cancel := context.WithCancel(context.Background())
	var totalPVCDeleted int = 0
	defer cancel()
	defer wg.Done()
	for index := 1; index <= worker; index++ {
		pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
		ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
			storageclass.Name, ds, pvclaimlabels, accessMode))
		pvclaim, err := fpv.CreatePVC(ctx, client, pvcnamespace, pvcspec)
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
			_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsToDelete, framework.ClaimProvisionTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaimToDelete.Name, pvcnamespace)
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
	namespace string) (*appsv1.StatefulSet, *v1.Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mkpath := func(file string) string {
		return filepath.Join(manifestPath, file)
	}
	statefulSet, err := manifest.StatefulSetFromManifest(mkpath("statefulset.yaml"), namespace)
	if err != nil {
		return nil, nil, err
	}
	service, err := manifest.SvcFromManifest(mkpath("service.yaml"))
	if err != nil {
		return nil, nil, err
	}
	service, err = client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			framework.Logf("services 'nginx' already exists")
		} else {
			return nil, nil, fmt.Errorf("failed to create nginx service: %v", err)
		}
	}
	replicas := int32(1)
	statefulSet.Spec.Replicas = &replicas
	_, err = client.AppsV1().StatefulSets(namespace).Create(ctx, statefulSet, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, err
	}
	return statefulSet, service, nil
}

// updateDeploymentReplicawithWait helps to update the replica for a deployment
// with wait.
func updateDeploymentReplicawithWait(client clientset.Interface, count int32, name string, namespace string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deployment *appsv1.Deployment
	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, healthStatusPollInterval, healthStatusPollTimeout, true,
		func(ctx context.Context) (bool, error) {
			deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if count == 0 && apierrors.IsNotFound(err) {
					return true, nil
				} else {
					return false, err
				}
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
func bringDownTKGController(Client clientset.Interface, vsphereTKGSystemNamespace string) {
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
func bringUpTKGController(Client clientset.Interface, tkgReplica int32, vsphereTKGSystemNamespace string) {
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
	waitErr := wait.PollUntilContextTimeout(ctx, healthStatusPollInterval, healthStatusPollTimeout, true,
		func(ctx context.Context) (bool, error) {
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
	list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, csiSystemNamespace, ignoreLabels)
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
	err = fpod.WaitForPodsRunningReady(ctx, client, csiSystemNamespace, int(num_csi_pods),
		time.Duration(2*pollTimeout))
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
			Resources: v1.VolumeResourceRequirements{
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
			Resources: v1.VolumeResourceRequirements{
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
func invokeVCenterReboot(ctx context.Context, host string) error {
	sshCmd := "reboot"
	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
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

// invokeVCenterServiceControl invokes the given command for the given service
// via service-control on the given vCenter host over SSH.
func invokeVCenterServiceControl(ctx context.Context, command, service, host string) error {
	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
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

// waitVCenterServiceToBeInState invokes the status check for the given service and waits
// via service-control on the given vCenter host over SSH.
func waitVCenterServiceToBeInState(ctx context.Context, serviceName string, host string, state string) error {

	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeoutShort*2, true,
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

// checkVcenterServicesStatus checks and polls for vCenter essential services status
// to be in running state
func checkVcenterServicesRunning(
	ctx context.Context, host string, essentialServices []string, timeout ...time.Duration) {

	var pollTime time.Duration
	if len(timeout) == 0 {
		pollTime = pollTimeout * 6
	} else {
		pollTime = timeout[0]
	}

	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTime, true,
		func(ctx context.Context) (bool, error) {
			var runningServices []string
			var statusMap = make(map[string]bool)
			allServicesRunning := true
			sshCmd := fmt.Sprintf("service-control --%s", statusOperation)
			framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
			result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
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
	// Checking for any extra services which needs to be started if in stopped or pending state after vc reboot
	err = checkVcServicesHealthPostReboot(ctx, host, timeout...)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(),
		"Got timed-out while waiting for all required VC services to be up and running")
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

	// get gc and validate if gc is deleted
	req, err = http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)

	err = waitForDeleteToComplete(client, req)

	return err

}

// waitForDeleteToComplete method polls for the requested object status
// returns true if its deleted successfully else returns error
func waitForDeleteToComplete(client *http.Client, req *http.Request) error {
	waitErr := wait.PollUntilContextTimeout(context.Background(), pollTimeoutShort, pollTimeout*6, true,
		func(ctx context.Context) (bool, error) {
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
func createGC(wcpHost string, wcpToken string, tkgImageName string, clusterName string) {

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

	var tkg TanzuCluster
	err = yaml.Unmarshal([]byte(gcBytes), &tkg)
	if err != nil {
		framework.Logf("Error: %v", err)
	}

	// Change the value of the replaceImage field
	tkg.Spec.Topology.ControlPlane.TKR.Reference.Name = tkgImageName
	tkg.Metadata.Name = clusterName

	// Marshal the updated struct back to YAML
	updatedYAML, err := yaml.Marshal(&tkg)
	if err != nil {
		framework.Logf("Error: %v", err)
	}

	// Convert the marshalled YAML to []byte
	updatedYAMLBytes := []byte(updatedYAML)

	req, err := http.NewRequest("POST", createGCURL, bytes.NewBuffer(updatedYAMLBytes))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", "Bearer "+wcpToken)
	req.Header.Add("Accept", "application/yaml")
	req.Header.Add("Content-Type", "application/yaml")
	bodyBytes, statusCode := httpRequest(client, req)

	response := string(bodyBytes)
	framework.Logf("%q", response)
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

	waitErr := wait.PollUntilContextTimeout(context.Background(), pollTimeoutShort, pollTimeoutShort*5, true,
		func(ctx context.Context) (bool, error) {
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

	waitErr := wait.PollUntilContextTimeout(context.Background(), pollTimeoutShort, pollTimeout*6, true,
		func(ctx context.Context) (bool, error) {
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
	framework.Logf("%q", response)
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

// getWindowsFileSystemSize finds the windowsWorkerIp and returns the size of the volume
func getWindowsFileSystemSize(client clientset.Interface, pod *v1.Pod) (int64, error) {
	var err error
	var output fssh.Result
	var windowsWorkerIP, size string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeName := pod.Spec.NodeName
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, node := range nodes.Items {
		if node.Name == nodeName {
			windowsWorkerIP = getK8sNodeIP(&node)
			break
		}
	}
	cmd := "Get-Disk | Format-List -Property Manufacturer,Size"
	if guestCluster {
		svcMasterIp := GetAndExpectStringEnvVar(svcMasterIP)
		svcMasterPwd := GetAndExpectStringEnvVar(svcMasterPassword)
		svcNamespace = GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
		sshWcpConfig := &ssh.ClientConfig{
			User: rootUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		output, err = execCommandOnGcWorker(sshWcpConfig, svcMasterIp, windowsWorkerIP,
			svcNamespace, cmd)
	} else {
		nimbusGeneratedWindowsVmPwd := GetAndExpectStringEnvVar(envWindowsPwd)
		windowsUser := GetAndExpectStringEnvVar(envWindowsUser)
		sshClientConfig := &ssh.ClientConfig{
			User: windowsUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedWindowsVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		output, err = sshExec(sshClientConfig, windowsWorkerIP, cmd)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	fullStr := strings.Split(strings.TrimSuffix(string(output.Stdout), "\n"), "\n")
	var originalSizeInbytes int64
	for index, line := range fullStr {
		if strings.Contains(line, "VMware") {
			sizeList := strings.Split(fullStr[index+1], ":")
			size = strings.TrimSpace(sizeList[1])
			originalSizeInbytes, err = strconv.ParseInt(size, 10, 64)
			if err != nil {
				return -1, fmt.Errorf("failed to parse size %s into int size", size)
			}
			if guestCluster {
				if originalSizeInbytes < 42949672960 {
					break
				}
			} else {
				if originalSizeInbytes < 96636764160 {
					break
				}
			}
		}
	}
	framework.Logf("disk size is  %d", originalSizeInbytes)
	return originalSizeInbytes, nil
}

// performPasswordRotationOnSupervisor invokes the given command to replace the password
//
//	Vmon-cli is used to restart the wcp service after changing the time.
func performPasswordRotationOnSupervisor(client clientset.Interface, ctx context.Context,
	csiNamespace string, host string) (bool, error) {

	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	host = ip + ":" + portNum

	// getting supervisorID and password
	vsphereCfg, err := getSvcConfigSecretData(client, ctx, csiNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	oldPassword := vsphereCfg.Global.Password
	// stopping wcp service
	sshCmd := fmt.Sprintf("vmon-cli --stop %s", wcpServiceName)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
	time.Sleep(sleepTimeOut)
	if err != nil || result.Code != 0 {
		return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	// To set last_storage_pwd_rotation_timestamp with 12 hrs prior to current timestamp
	currentTimestamp := time.Now()
	twelveHoursAgoTimestamp := (currentTimestamp.Add(-12 * time.Hour)).Unix()
	sshCmd = fmt.Sprintf("cd /etc/vmware/wcp; sudo -u wcp psql -U wcpuser -d VCDB -c "+
		"\"update vcenter_svc_accounts set last_pwd_rotation_timestamp=%v "+
		"where instance_id='%s'\"", twelveHoursAgoTimestamp, vsphereCfg.Global.SupervisorID)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err = fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	// Starting wcp service
	sshCmd = fmt.Sprintf("vmon-cli --start %s", wcpServiceName)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err = fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
	time.Sleep(sleepTimeOut)
	if err != nil || result.Code != 0 {
		return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	// waiting for the changed password to reflect in CSI config file
	waitErr := wait.PollUntilContextTimeout(ctx, pollTimeoutShort, pwdRotationTimeout, true,
		func(ctx context.Context) (bool, error) {
			vsphereCfg, err := getSvcConfigSecretData(client, ctx, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			changedPassword := vsphereCfg.Global.Password
			framework.Logf("Password from Config Secret of svc: %v", changedPassword)
			if changedPassword != oldPassword {
				return true, nil
			}
			return false, nil
		})
	return true, waitErr
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
func invokeVCenterChangePassword(ctx context.Context, user, adminPassword, newPassword,
	host string, clientIndex int) error {
	var copyCmd string
	var removeCmd string
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
	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	// Remote copy this input file to VC.
	if !multivc {
		copyCmd = fmt.Sprintf("/bin/cat %s | /usr/bin/ssh root@%s '/usr/bin/cat >> input_copy.txt'",
			path, e2eVSphere.Config.Global.VCenterHostname)
	} else {
		vCenter := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")[clientIndex]
		copyCmd = fmt.Sprintf("/bin/cat %s | /usr/bin/ssh root@%s '/usr/bin/cat >> input_copy.txt'",
			path, vCenter)
	}
	fmt.Printf("Executing the command: %s\n", copyCmd)
	_, err = exec.Command("/bin/sh", "-c", copyCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		// Remove the input_copy.txt file from VC.
		if !multivc {
			removeCmd = fmt.Sprintf("/usr/bin/ssh root@%s '/usr/bin/rm input_copy.txt'",
				vcAddress)
		} else {
			vCenter := strings.Split(multiVCe2eVSphere.multivcConfig.Global.VCenterHostname, ",")[clientIndex]
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
			podRegion := node.Labels[regionKey]
			podZone := node.Labels[zoneKey]
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

	var storagePolicyNameForSharedDatastores string
	var storagePolicyNameForSvc1 string
	var storagePolicyNameForSvc2 string

	waitTime := 15
	var executeCreateResourceQuota bool
	executeCreateResourceQuota = true
	if !multipleSvc {
		// reading export variable from a single supervisor cluster
		storagePolicyNameForSharedDatastores = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	} else {
		// reading multiple export variables from a multi svc setup
		storagePolicyNameForSvc1 = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDsSvc1)
		storagePolicyNameForSvc2 = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDsSvc2)
	}

	if supervisorCluster {
		_, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, namespace+"-storagequota", metav1.GetOptions{})
		if !multipleSvc {
			if err != nil || !(scName == storagePolicyNameForSharedDatastores) {
				executeCreateResourceQuota = true
			} else {
				executeCreateResourceQuota = false
			}
		} else {
			if err != nil || !(scName == storagePolicyNameForSvc1) || !(scName == storagePolicyNameForSvc2) {
				executeCreateResourceQuota = true
			} else {
				executeCreateResourceQuota = false
			}
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

	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeoutShort, true,
		func(ctx context.Context) (bool, error) {
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
	svcPvclaim, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).
		Get(context.TODO(), pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	svcPV, err := svcClient.CoreV1().PersistentVolumes().
		Get(context.TODO(), svcPvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	svcPvclaim, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).
		Get(context.TODO(), pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	svcPV, err := svcClient.CoreV1().PersistentVolumes().
		Get(context.TODO(), svcPvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return svcPV
}

// verfiy File exists or not in vSphere Volume
// TODO : add logic to disable disk caching in windows
func verifyFilesExistOnVSphereVolume(namespace string, podName string, poll, timeout time.Duration,
	filePaths ...string) {
	var err error
	for _, filePath := range filePaths {
		if windowsEnv {
			for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
				_, err = e2eoutput.LookForStringInPodExec(namespace, podName,
					[]string{"powershell.exe", "Test-Path -path", filePath}, "", time.Minute)
				if err == nil {
					break
				} else if err != nil {
					framework.Logf("File %s doesn't exist", filePath)
					continue
				}
			}
		} else {
			_, err = e2ekubectl.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace),
				podName, "--", "/bin/ls", filePath)
		}
		framework.ExpectNoError(err, fmt.Sprintf("failed to verify file: %q on the pod: %q", filePath, podName))
	}
}

// verify File System type on vSphere volume
func verifyFsTypeOnVsphereVolume(namespace string, podName string, expectedContent string, filePaths ...string) {
	var err error
	for _, filePath := range filePaths {
		if windowsEnv {
			filePath = filePath + ".txt"
			// TODO : add logic to disable disk caching in windows
			verifyFilesExistOnVSphereVolume(namespace, podName, poll, pollTimeoutShort, filePath)
			_, err = e2eoutput.LookForStringInPodExec(namespace, podName,
				[]string{"powershell.exe", "cat", filePath}, ntfsFSType, time.Minute)
		} else {
			_, err = e2eoutput.LookForStringInPodExec(namespace, podName,
				[]string{"/bin/cat", filePath}, expectedContent, time.Minute)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func createEmptyFilesOnVSphereVolume(namespace string, podName string, filePaths []string) {
	var err error
	for _, filePath := range filePaths {
		if windowsEnv {
			_, err = e2eoutput.LookForStringInPodExec(namespace, podName,
				[]string{"powershell.exe", "New-Item", "-Path", filePath, "-ItemType File"}, "", time.Minute)
		} else {
			err = e2eoutput.CreateEmptyFileOnPod(namespace, podName, filePath)
		}
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
	if windowsEnv {
		ss.Spec.Template.Spec.Containers[0].Image = windowsImageOnMcr
		ss.Spec.Template.Spec.Containers[0].Command = []string{"Powershell.exe"}
		ss.Spec.Template.Spec.Containers[0].Args = []string{"-Command", windowsExecCmd}
	}
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
func verifyPodCreation(ctx context.Context, f *framework.Framework, client clientset.Interface, namespace string,
	pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) {
	defer ginkgo.GinkgoRecover()
	ginkgo.By("Create pod and wait for this to be in running phase")
	pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
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
	err = fpod.DeletePodWithWait(ctx, client, pod)
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
func verifyCNSFileAccessConfigCRDInSupervisor(ctx context.Context,
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
				framework.Logf("CRD is not matching : %q", instance.Name)
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
	return wait.PollUntilContextTimeout(ctx, poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
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
	var netPerm NetPermissionConfig
	key, value := "", ""
	var permissions vsanfstypes.VsanFileShareAccessType
	var rootSquash bool
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
		case "query-limit":
			config.Global.QueryLimit, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "list-volume-threshold":
			config.Global.ListVolumeThreshold, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "ca-file":
			config.Global.CaFile = value
		case "supervisor-id":
			config.Global.SupervisorID = value
		case "targetvSANFileShareClusters":
			config.Global.TargetVsanFileShareClusters = value
		case "fileVolumeActivated":
			config.Global.FileVolumeActivated, strconvErr = strconv.ParseBool(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "ips":
			netPerm.Ips = value
		case "permissions":
			netPerm.Permissions = permissions
		case "rootsquash":
			netPerm.RootSquash = rootSquash
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
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"\n\n"+
		"[Snapshot]\nglobal-max-snapshots-per-block-volume = %d\n\n"+
		"[Labels]\ntopology-categories = \"%s\"",
		cfg.Global.InsecureFlag, cfg.Global.ClusterID, cfg.Global.ClusterDistribution,
		cfg.Global.CSIFetchPreferredDatastoresIntervalInMin, cfg.Global.QueryLimit,
		cfg.Global.ListVolumeThreshold,
		cfg.Global.VCenterHostname, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort,
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
	err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
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
	var shellExec, cmdArg, syncCmd string
	if windowsEnv {
		shellExec = "Powershell.exe"
		cmdArg = "-Command"
	} else {
		shellExec = "/bin/sh"
		cmdArg = "-c"
		syncCmd = fmt.Sprintf("echo '%s' >> %s && sync", data, filePath)
	}

	// Command to write data and sync it
	wrtiecmd := []string{"exec", podName, "--namespace=" + namespace, "--", shellExec, cmdArg, syncCmd}
	e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
}

// readFileFromPod read data from given Pod and the given file.
func readFileFromPod(namespace string, podName string, filePath string) string {
	var output string
	var shellExec, cmdArg string
	if windowsEnv {
		shellExec = "Powershell.exe"
		cmdArg = "-Command"
	} else {
		shellExec = "/bin/sh"
		cmdArg = "-c"
	}
	cmd := []string{"exec", podName, "--namespace=" + namespace, "--", shellExec, cmdArg,
		fmt.Sprintf("cat %s", filePath)}
	output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
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
			Resources: v1.VolumeResourceRequirements{
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

	err := psodHost(hostIP, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
			// Fix for NotAuthenticated issue
			bootstrap()

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

	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(addr)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	result := fssh.Result{Host: ip, Cmd: cmd}
	// Connect.
	client, err := ssh.Dial("tcp", net.JoinHostPort(ip, portNum), config)
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
			framework.Logf("%q", exiterr.Error())
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
	framework.Logf("hostd status command output is %q:", output)
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
			Resources: v1.VolumeResourceRequirements{
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
	waitErr := wait.PollUntilContextTimeout(ctx, poll, 2*pollTimeout, true,
		func(ctx context.Context) (bool, error) {
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
func bringSvcK8sAPIServerDown(ctx context.Context, vc string) error {
	file := "master.txt"
	token := "token.txt"

	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(vc)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vc = ip + ":" + portNum

	// Note: /usr/lib/vmware-wcp/decryptK8Pwd.py is not an officially supported
	// API and may change at any time.
	sshCmd := fmt.Sprintf("/usr/lib/vmware-wcp/decryptK8Pwd.py > %s", file)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err := fssh.SSH(ctx, sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf("(awk 'FNR == 7 {print $2}' %s) > %s", file, token)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err = fssh.SSH(ctx, sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	sshCmd = fmt.Sprintf(
		"sshpass -f %s ssh root@$(awk 'FNR == 6 {print $2}' master.txt) -o 'StrictHostKeyChecking no' 'mv %s/%s /root'",
		token, kubeAPIPath, kubeAPIfile)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err = fssh.SSH(ctx, sshCmd, vc, framework.TestContext.Provider)
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

	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(vc)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vc = ip + ":" + portNum

	sshCmd := fmt.Sprintf("sshpass -f token.txt ssh root@$(awk 'FNR == 6 {print $2}' master.txt) "+
		"-o 'StrictHostKeyChecking no' 'mv /root/%s %s'", kubeAPIfile, kubeAPIPath)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err := fssh.SSH(ctx, sshCmd, vc, framework.TestContext.Provider)
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
	waitErr := wait.PollUntilContextTimeout(ctx, healthStatusPollInterval, healthStatusPollTimeout, true,
		func(ctx context.Context) (bool, error) {
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
	ip, portNum, err := getPortNumAndIP(ip)
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
	framework.Logf("Waiting up to %v for CnsRegisterVolume %v, namespace: %s,  to get created",
		timeout, cnsRegisterVolume, namespace)

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

	describeCNSRegisterVolume(ctx, svcNamespace, cnsRegisterVolumeName)

	return fmt.Errorf("cnsRegisterVolume %s creation is failed within %v", cnsRegisterVolumeName, timeout)
}

// describe CNS RegisterVolume
func describeCNSRegisterVolume(ctx context.Context, namespace string, cnsRegVolName string) string {
	// Command to write data and sync it
	cmd := []string{"describe", "cnsregistervolumes.cns.vmware.com", cnsRegVolName}
	output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
	framework.Logf("Describe CNSRegistervolume : %s", output)

	return output
}

// waitForCNSRegisterVolumeToGetDeleted waits for a cnsRegisterVolume to get
// deleted or until timeout occurs, whichever comes first.
func waitForCNSRegisterVolumeToGetDeleted(ctx context.Context, restConfig *rest.Config, namespace string,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for cnsRegisterVolume %v to get deleted", timeout, cnsRegisterVolume)

	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		flag := queryCNSRegisterVolume(ctx, restConfig, cnsRegisterVolumeName, namespace)
		if flag {
			framework.Logf("CnsRegisterVolume %s is not yet deleted. Deletion flag status  =%t (%v)",
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
		_, err = fpod.WaitForPodsWithLabelRunningReady(ctx,
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
	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
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

// createPod with given claims based on node selector.
func createPod(ctx context.Context, client clientset.Interface, namespace string, nodeSelector map[string]string,
	pvclaims []*v1.PersistentVolumeClaim, isPrivileged bool, command string) (*v1.Pod, error) {
	securityLevel := api.LevelBaseline
	if isPrivileged {
		securityLevel = api.LevelPrivileged
	}
	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, securityLevel, command)
	if windowsEnv {
		var commands []string
		if (len(command) == 0) || (command == execCommand) {
			commands = []string{"Powershell.exe", "-Command ", windowsExecCmd}
		} else if command == execRWXCommandPod {
			commands = []string{"Powershell.exe", "-Command ", windowsExecRWXCommandPod}
		} else if command == execRWXCommandPod1 {
			commands = []string{"Powershell.exe", "-Command ", windowsExecRWXCommandPod1}
		} else {
			commands = []string{"Powershell.exe", "-Command", command}
		}
		pod.Spec.Containers[0].Image = windowsImageOnMcr
		pod.Spec.Containers[0].Command = commands
		pod.Spec.Containers[0].VolumeMounts[0].MountPath = pod.Spec.Containers[0].VolumeMounts[0].MountPath + "/"
	} else {
		pod.Spec.Containers[0].Image = busyBoxImageOnGcr
	}
	pod, err := client.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %v", err)
	}
	// Waiting for pod to be running.
	err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
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

func getDeploymentSpec(ctx context.Context, client clientset.Interface, replicas int32,
	podLabels map[string]string, nodeSelector map[string]string, namespace string,
	pvclaims []*v1.PersistentVolumeClaim, command string, isPrivileged bool, image string) *appsv1.Deployment {
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
	return deploymentSpec
}

// createDeployment create a deployment with 1 replica for given pvcs and node
// selector.
func createDeployment(ctx context.Context, client clientset.Interface, replicas int32,
	podLabels map[string]string, nodeSelector map[string]string, namespace string,
	pvclaims []*v1.PersistentVolumeClaim, command string, isPrivileged bool, image string) (*appsv1.Deployment, error) {
	if len(command) == 0 {
		if windowsEnv {
			command = windowsExecCmd
		} else {
			command = "trap exit TERM; while true; do sleep 1; done"
		}
	}
	deploymentSpec := getDeploymentSpec(ctx, client, replicas, podLabels, nodeSelector, namespace,
		pvclaims, command, isPrivileged, image)
	if windowsEnv {
		var commands []string
		if (len(command) == 0) || (command == execCommand) {
			commands = []string{windowsExecCmd}
		} else if command == execRWXCommandPod {
			commands = []string{windowsExecRWXCommandPod}
		} else if command == execRWXCommandPod1 {
			commands = []string{windowsExecRWXCommandPod1}
		} else {
			commands = []string{command}
		}
		deploymentSpec.Spec.Template.Spec.Containers[0].Image = windowsImageOnMcr
		deploymentSpec.Spec.Template.Spec.Containers[0].Command = []string{"Powershell.exe"}
		deploymentSpec.Spec.Template.Spec.Containers[0].Args = commands
	}
	deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return deployment, fmt.Errorf("deployment %q Create API error: %v", deploymentSpec.Name, err)
	}
	framework.Logf("Waiting deployment %q to complete", deploymentSpec.Name)
	err = fdep.WaitForDeploymentComplete(client, deployment)
	if err != nil {
		return deployment, fmt.Errorf("deployment %q failed to complete: %v", deploymentSpec.Name, err)
	}
	return deployment, nil
}

// createPodForFSGroup helps create pod with fsGroup.
func createPodForFSGroup(ctx context.Context, client clientset.Interface, namespace string,
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
	securityLevel := api.LevelBaseline
	if isPrivileged {
		securityLevel = api.LevelPrivileged
	}
	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, securityLevel, command)
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
	err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
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
	waitErr := wait.PollUntilContextTimeout(ctx, poll*15, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
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
			framework.Logf("Looking for default datastore in DC: %q", dc)
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			if err == nil {
				framework.Logf("Datstore found for DS URL:%q", datastoreURL)
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
		pods, err := fpod.GetPodsInNamespace(ctx, client, namespace, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(ctx, client, namespace, int(len(pods)),
			time.Duration(pollTimeout*2))
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
	waitErr := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
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
	addressFound := false
	addrs := node.Status.Addresses
	for _, addr := range addrs {
		if addr.Type == v1.NodeExternalIP && (net.ParseIP(addr.Address)).To4() != nil {
			address = addr.Address
			addressFound = true
			break
		}
	}
	if !addressFound {
		for _, addr := range addrs {
			if addr.Type == v1.NodeInternalIP && (net.ParseIP(addr.Address)).To4() != nil {
				address = addr.Address
				break
			}
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
	waitErr := wait.PollUntilContextTimeout(ctx, healthStatusPollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
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

	if windowsEnv {
		ss.Spec.Template.Spec.Containers[0].Image = windowsImageOnMcr
		ss.Spec.Template.Spec.Containers[0].Command = []string{"Powershell.exe"}
		ss.Spec.Template.Spec.Containers[0].Args = []string{"-Command", windowsExecCmd}
	}
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

		// Collect Pod logs
		for _, cont := range pod.Spec.Containers {
			output, err := fpod.GetPodLogs(ctx, client, pod.Namespace, pod.Name, cont.Name)
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
	ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				// verify pv node affinity details
				pvRegion, pvZone, err = verifyVolumeTopology(pv, zoneValues, regionValues)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
				framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
				if !(len(nodeList.Items) > 0) {
					framework.Failf("Unable to find ready and schedulable Node")
				}
				// verify node topology details
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
func createTopologyMapLevel5(topologyMapStr string) (map[string][]string, []string) {
	topologyMap := make(map[string][]string)
	var categories []string
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
func createAllowedTopolgies(topologyMapStr string) []v1.TopologySelectorLabelRequirement {
	topologyFeature := os.Getenv(topologyFeature)
	topologyMap, _ := createTopologyMapLevel5(topologyMapStr)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{}
	topoKey := ""
	if topologyFeature == topologyTkgHaName || topologyFeature == podVMOnStretchedSupervisor ||
		topologyFeature == topologyDomainIsolation {
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
				framework.Logf("pv.Spec.NodeAffinity: %v, nodeSelector: %v", pv.Spec.NodeAffinity, nodeSelector)
				if !compareStringLists(val, topology.Values) {
					if topologyFeature == topologyTkgHaName ||
						topologyFeature == podVMOnStretchedSupervisor ||
						topologyFeature == topologyDomainIsolation {
						return false, fmt.Errorf("pv node affinity details: %v does not match"+
							"with: %v in the allowed topologies", topology.Values, val)
					} else {
						return false, fmt.Errorf("PV node affinity details does not exist in the allowed " +
							"topologies specified in SC")
					}
				}
			} else {
				if topologyFeature == topologyTkgHaName ||
					topologyFeature == podVMOnStretchedSupervisor ||
					topologyFeature == topologyDomainIsolation {
					return false, fmt.Errorf("pv node affinity key: %v does not does not exist in the "+
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
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	parallelStatefulSetCreation bool) error {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	var ssPodsBeforeScaleDown *v1.PodList

	var err error
	adminClient, client := initializeClusterClientsByUserRoles(client)
	if parallelStatefulSetCreation {
		ssPodsBeforeScaleDown = GetListOfPodsInSts(client, statefulset)
	} else {
		ssPodsBeforeScaleDown, err = fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err = client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				// get pv details
				pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

				// verify pv node affinity details as specified on SC
				ginkgo.By("Verifying PV node affinity details")
				res, err := verifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
				if res {
					framework.Logf("PV %s node affinity details lie in the specified allowed "+
						"topologies of Storage Class", pv.Name)
				}
				if !res {
					return fmt.Errorf("PV %s node affinity details are not in the specified allowed "+
						"topologies of Storage Class", pv.Name)
				}
				if err != nil {
					return err
				}

				// fetch node details
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, adminClient)
				if err != nil {
					return err
				}
				if len(nodeList.Items) <= 0 {
					return fmt.Errorf("unable to find ready and schedulable Node")
				}

				// verify pod is running on appropriate nodes
				ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
				res, err = verifyPodLocationLevel5(&sspod, nodeList, allowedTopologiesMap)
				if res {
					framework.Logf("Pod %v is running on an appropriate node as specified in the "+
						"allowed topologies of Storage Class", sspod.Name)
				}
				if !res {
					return fmt.Errorf("pod %v is not running on an appropriate node as specified "+
						"in the allowed topologies of Storage Class", sspod.Name)
				}
				if err != nil {
					return err
				}

				// Verify the attached volume match the one in CNS cache
				if !multivc {
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				} else {
					err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				}

			}
		}
	}

	return nil
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
					if !isValuePresentInTheList(topologyValue, labelValue) {
						return false, fmt.Errorf("pod: %s is not running on node located in %s", pod.Name, labelValue)
					}
				}
			}
		}
	}
	return true, nil
}

/* This util will fetch list of nodes from  a particular zone*/
func fetchAllNodesOfSpecificZone(nodeList *v1.NodeList,
	allowedTopologiesMap map[string][]string) *v1.NodeList {

	// Create a new NodeList to hold the matching nodes
	filteredNodes := &v1.NodeList{}

	for _, node := range nodeList.Items {
		for labelKey, allowedValues := range allowedTopologiesMap {
			if nodeValue, ok := node.Labels[labelKey]; ok {
				if isValuePresentInTheList(allowedValues, nodeValue) {
					filteredNodes.Items = append(filteredNodes.Items, node)
					break
				}
			}
		}
	}

	return filteredNodes
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
	pollErr := wait.PollUntilContextTimeout(context.Background(), StatefulSetPoll, StatefulSetTimeout, true,
		func(ctx context.Context) (bool, error) {
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
scaleDownStatefulSetPod util is used to perform scale down operation on StatefulSert Pods
and later verifies that after scale down operation volume gets detached from the node
and returns nil if no error found
*/
func scaleDownStatefulSetPod(ctx context.Context, client clientset.Interface,
	statefulset *appsv1.StatefulSet, namespace string, replicas int32, parallelStatefulSetCreation bool) error {
	ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
	var ssPodsAfterScaleDown *v1.PodList
	var err error

	if parallelStatefulSetCreation {
		_, scaledownErr := scaleStatefulSetPods(client, statefulset, replicas)
		if scaledownErr != nil {
			return scaledownErr
		}
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown = GetListOfPodsInSts(client, statefulset)
	} else {
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas)
		if scaledownErr != nil {
			return scaledownErr
		}
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown, err = fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// After scale down, verify vSphere volumes are detached from deleted pods
	ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
	for _, sspod := range ssPodsAfterScaleDown.Items {
		_, err = client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if !multivc {
							isDiskDetached, detachErr := e2eVSphere.waitForVolumeDetachedFromNode(
								client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							if detachErr != nil {
								return detachErr
							}
							if !isDiskDetached {
								return fmt.Errorf("volume %q is not detached from the node %q",
									pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							}
						} else {
							isDiskDetached, detachErr := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(
								client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							if detachErr != nil {
								return detachErr
							}
							if !isDiskDetached {
								return fmt.Errorf("volume %q is not detached from the node %q",
									pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							}
						}
					}
				}
			} else {
				return err
			}
		}
	}
	// After scale down, verify the attached volumes match those in CNS Cache
	for _, sspod := range ssPodsAfterScaleDown.Items {
		_, err = client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				if !multivc {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				} else {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

/*
scaleUpStatefulSetPod util is used to perform scale up operation on StatefulSert Pods
and later verifies that after scale up operation volume get successfully attached to the node
and returns nil if no error found
*/
func scaleUpStatefulSetPod(ctx context.Context, client clientset.Interface,
	statefulset *appsv1.StatefulSet, namespace string, replicas int32,
	parallelStatefulSetCreation bool) error {
	ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
	var ssPodsAfterScaleUp *v1.PodList
	var err error

	if parallelStatefulSetCreation {
		_, scaleupErr := scaleStatefulSetPods(client, statefulset, replicas)
		if scaleupErr != nil {
			return scaleupErr
		}
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

		ssPodsAfterScaleUp = GetListOfPodsInSts(client, statefulset)
		if len(ssPodsAfterScaleUp.Items) == 0 {
			return fmt.Errorf("unable to get list of Pods from the Statefulset: %v", statefulset.Name)
		}
		if len(ssPodsAfterScaleUp.Items) != int(replicas) {
			return fmt.Errorf("number of Pods in the statefulset should match with number of replicas")
		}
	} else {
		_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
		if scaleupErr != nil {
			return scaleupErr
		}
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

		ssPodsAfterScaleUp, err = fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if len(ssPodsAfterScaleUp.Items) == 0 {
			return fmt.Errorf("unable to get list of Pods from the Statefulset: %v", statefulset.Name)
		}
		if len(ssPodsAfterScaleUp.Items) != int(replicas) {
			return fmt.Errorf("number of Pods in the statefulset should match with number of replicas")
		}
	}

	// After scale up, verify all vSphere volumes are attached to node VMs.
	ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
	for _, sspod := range ssPodsAfterScaleUp.Items {
		err = fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace, pollTimeout)
		if err != nil {
			return err
		}
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
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
				} else if supervisorCluster {
					annotations := pod.Annotations
					vmUUID, exists = annotations[vmUUIDLabel]
					if !exists {
						return fmt.Errorf("pod doesn't have %s annotation", vmUUIDLabel)
					}
					if !multivc {
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						if err != nil {
							return err
						}
					} else {
						_, err := multiVCe2eVSphere.getVMByUUIDForMultiVC(ctx, vmUUID)
						if err != nil {
							return err
						}
					}
				}
				if !guestCluster {
					if !multivc {
						if !rwxAccessMode {
							isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle, vmUUID)
							if err != nil {
								return err
							}
							if !isDiskAttached {
								return fmt.Errorf("disk is not attached to the node")
							}
						}
						err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						if err != nil {
							return err
						}
					} else {
						if !rwxAccessMode {
							isDiskAttached, err := multiVCe2eVSphere.verifyVolumeIsAttachedToVMInMultiVC(pv.Spec.CSI.VolumeHandle, vmUUID)
							if err != nil {
								return err
							}
							if !isDiskAttached {
								return fmt.Errorf("disk is not attached to the node")
							}
						}
						err = verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
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
	if topologyFeature == topologyTkgHaName || topologyFeature == podVMOnStretchedSupervisor {
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
			if topologyFeature == topologyTkgHaName || topologyFeature == podVMOnStretchedSupervisor {
				values = topologyAffinityDetails[key+"/"+category]
				framework.Logf("values: %v", values)
			} else {
				values = topologyAffinityDetails[category]
				framework.Logf("values: %v", values)
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
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	parallelDeplCreation bool) error {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	var pods *v1.PodList
	var err error

	if parallelDeplCreation {
		pods, err = GetPodsForMultipleDeployment(client, deployment)
		if err != nil {
			return err
		}
	} else {
		pods, err = fdep.GetPodsForDeployment(ctx, client, deployment)
		if err != nil {
			return err
		}
	}

	for _, sspod := range pods.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				// get pv details
				pv := getPvFromClaim(client, deployment.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

				// verify pv node affinity details as specified on SC
				ginkgo.By("Verifying PV node affinity details")
				res, err := verifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
				if res {
					framework.Logf("PV %s node affinity details lie in the specified allowed "+
						"topologies of Storage Class", pv.Name)
				}
				if !res {
					return fmt.Errorf("PV %s node affinity details are not in the specified allowed "+
						"topologies of Storage Class", pv.Name)
				}
				if err != nil {
					return err
				}

				// fetch node details
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
				if err != nil {
					return err
				}
				if len(nodeList.Items) <= 0 {
					return fmt.Errorf("unable to find ready and schedulable Node")
				}

				// verify pod is running on appropriate nodes
				ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
				res, err = verifyPodLocationLevel5(&sspod, nodeList, allowedTopologiesMap)
				if res {
					framework.Logf("Pod %v is running on an appropriate node as specified in the "+
						"allowed topologies of Storage Class", sspod.Name)
				}
				if !res {
					return fmt.Errorf("pod %v is not running on an appropriate node as specified in the "+
						"allowed topologies of Storage Class", sspod.Name)
				}
				if err != nil {
					return err
				}

				// Verify the attached volume match the one in CNS cache
				if !multivc {
					err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				} else {
					err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

/*
For Standalone Pod
verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5
verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5 for Standalone Pod verifies that PV
node Affinity rules should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on which PV
is provisioned.
*/
func verifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx context.Context,
	client clientset.Interface, pod *v1.Pod,
	allowedTopologies []v1.TopologySelectorLabelRequirement) error {
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	for _, volumespec := range pod.Spec.Volumes {
		if volumespec.PersistentVolumeClaim != nil {
			// get pv details
			pv := getPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
			if pv == nil {
				return fmt.Errorf("failed to get PV for claim: %s", volumespec.PersistentVolumeClaim.ClaimName)
			}

			// verify pv node affinity details as specified on SC
			ginkgo.By("Verifying PV node affinity details")
			res, err := verifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
			if err != nil {
				return fmt.Errorf("error verifying PV node affinity: %v", err)
			}
			if !res {
				return fmt.Errorf("PV %s node affinity details are not in the specified allowed "+
					"topologies of Storage Class", pv.Name)
			}

			// fetch node details
			nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
			if err != nil {
				return fmt.Errorf("error getting ready and schedulable nodes: %v", err)
			}
			if !(len(nodeList.Items) > 0) {
				return errors.New("no ready and schedulable nodes found")
			}

			// verify pod is running on appropriate nodes
			ginkgo.By("Verifying If Pods are running on appropriate nodes as mentioned in SC")
			res, err = verifyPodLocationLevel5(pod, nodeList, allowedTopologiesMap)
			if err != nil {
				return fmt.Errorf("error verifying pod location: %v", err)
			}
			if !res {
				return fmt.Errorf("pod %v is not running on appropriate node as specified in allowed "+
					"topologies of Storage Class", pod.Name)
			}

			// Verify the attached volume matches the one in CNS cache
			if !multivc {
				err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
				if err != nil {
					return fmt.Errorf("error verifying volume metadata in CNS: %v", err)
				}
			} else {
				err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
				if err != nil {
					return fmt.Errorf("error verifying volume metadata in CNS for multi-VC: %v", err)
				}
			}
		}
	}
	return nil
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
	rackTopology := allowedTopologies[len(allowedTopologies)-1]

	for i := 0; i < len(allowedTopologies)-1; i++ {
		topologySelector := allowedTopologies[i]
		var nodeSelectorRequirement v1.NodeSelectorRequirement
		nodeSelectorRequirement.Key = topologySelector.Key
		nodeSelectorRequirement.Operator = "In"
		nodeSelectorRequirement.Values = topologySelector.Values
		nodeSelectorRequirements = append(nodeSelectorRequirements, nodeSelectorRequirement)
	}

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

// getK8sMasterNodeIPWhereControllerLeaderIsRunning fetches the master node IP
// where controller is running
func getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx context.Context,
	client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	containerName string) (string, string, error) {
	ignoreLabels := make(map[string]string)
	csiControllerPodName, grepCmdForFindingCurrentLeader := "", ""
	csiPods, err := fpod.GetPodsInNamespace(ctx, client, csiSystemNamespace, ignoreLabels)
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
		if strings.Contains(cmdResult.Stderr, "OCI runtime resume failed") ||
			strings.Contains(cmdResult.Stderr, "cannot resume a stopped container: unknown") {
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

func createParallelStatefulSets(client clientset.Interface, namespace string,
	statefulset *appsv1.StatefulSet, replicas int32, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	ginkgo.By("Creating statefulset")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf("Creating statefulset %v/%v with %d replicas and selector %+v",
		statefulset.Namespace, statefulset.Name, replicas, statefulset.Spec.Selector)
	_, err := client.AppsV1().StatefulSets(namespace).Create(ctx, statefulset, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func createParallelStatefulSetSpec(namespace string, no_of_sts int, replicas int32) []*appsv1.StatefulSet {
	stss := []*appsv1.StatefulSet{}
	var statefulset *appsv1.StatefulSet

	for i := 0; i < no_of_sts; i++ {
		scName := defaultNginxStorageClassName
		statefulset = GetStatefulSetFromManifest(namespace)
		statefulset.Name = "thread-" + strconv.Itoa(i) + "-" + statefulset.Name
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &scName
		statefulset.Spec.Replicas = &replicas
		stss = append(stss, statefulset)
	}
	return stss
}

func createMultiplePVCsInParallel(ctx context.Context, client clientset.Interface, namespace string,
	storageclass *storagev1.StorageClass, count int, pvclaimlabels map[string]string) []*v1.PersistentVolumeClaim {
	var pvclaims []*v1.PersistentVolumeClaim
	for i := 0; i < count; i++ {
		pvclaim, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
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
		stdout, err := e2eoutput.RunHostCmdWithRetries(statefulPod.Namespace,
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
	adminClient, _ := initializeClusterClientsByUserRoles(client)
	csiPods, err := fpod.GetPodsInNamespace(ctx, adminClient, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	num_csi_pods := len(csiPods)
	// Collecting and dumping csi pod logs before deleting them
	collectPodLogs(ctx, adminClient, csiSystemNamespace)
	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, vSphereCSIControllerPodNamePrefix) && csiPod.Name == csi_controller_pod {
			framework.Logf("Deleting the pod: %s", csiPod.Name)
			err = fpod.DeletePodWithWait(ctx, adminClient, csiPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	// wait for csi Pods to be in running ready state
	err = fpod.WaitForPodsRunningReady(ctx, adminClient, csiSystemNamespace, int(num_csi_pods),
		time.Duration(pollTimeout))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return nil
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
				err = waitForHostToBeDown(ctx, esxInfo["ip"])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
	return powerOffHostsList
}

// waitForPvcToBeDeleted waits by polling for a particular pvc to be deleted in a namespace
func waitForPvcToBeDeleted(ctx context.Context, client clientset.Interface, pvcName string, namespace string) error {
	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			_, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					framework.Logf("PVC is deleted: %v", pvcName)
					return true, nil
				} else {
					return false, fmt.Errorf("pvc %s is still not deleted in"+
						"namespace %s with err: %v", pvcName, namespace, err)
				}
			}
			return false, nil
		})
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
	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeoutShort, true,
		func(ctx context.Context) (bool, error) {
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
func stopCSIPods(ctx context.Context, client clientset.Interface, namespace string) (bool, error) {
	collectPodLogs(ctx, client, csiSystemNamespace)
	isServiceStopped := false
	err := updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix,
		namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = true
	return isServiceStopped, err
}

// startCSIPods function starts the csi pods and waits till all the pods comes up
func startCSIPods(ctx context.Context, client clientset.Interface, csiReplicas int32,
	namespace string) (bool, error) {
	ignoreLabels := make(map[string]string)
	err := updateDeploymentReplicawithWait(client, csiReplicas, vSphereCSIControllerPodNamePrefix,
		namespace)
	if err != nil {
		return true, err
	}
	// Wait for the CSI Pods to be up and Running
	list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, namespace, ignoreLabels)
	if err != nil {
		return true, err
	}
	num_csi_pods := len(list_of_pods)
	err = fpod.WaitForPodsRunningReady(ctx, client, namespace, int(num_csi_pods),
		time.Duration(pollTimeout))
	isServiceStopped := false
	return isServiceStopped, err
}

// waitForStsPodsToBeInRunningState function waits till all the pods comes up
func waitForStsPodsToBeInReadyRunningState(ctx context.Context, client clientset.Interface, namespace string,
	statefulSets []*appsv1.StatefulSet) error {
	waitErr := wait.PollUntilContextTimeout(ctx, pollTimeoutShort, pollTimeoutShort*20, true,
		func(ctx context.Context) (bool, error) {
			for i := 0; i < len(statefulSets); i++ {
				fss.WaitForStatusReadyReplicas(ctx, client, statefulSets[i], *statefulSets[i].Spec.Replicas)
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
				fpod.DeletePodOrFail(ctx, client, csiSystemNamespace, pod.Name)
			}
			err = fpod.WaitForPodsRunningReady(ctx, client, csiSystemNamespace, int(len(csipods.Items)),
				time.Duration(pollTimeout))
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
func triggerFullSync(ctx context.Context, cnsOperatorClient client.Client) {
	err := waitForFullSyncToFinish(ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
	crd := getTriggerFullSyncCrd(ctx, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)
	updateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd)
	err = waitForFullSyncToFinish(ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
	crd_updated := getTriggerFullSyncCrd(ctx, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)

	updateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd_updated)
	err = waitForFullSyncToFinish(ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
}

// waitAndGetContainerID waits and fetches containerID of a given containerName
func waitAndGetContainerID(sshClientConfig *ssh.ClientConfig, k8sMasterIP string,
	containerName string, k8sVersion float64) (string, error) {
	containerId := ""
	cmdToGetContainerId := ""
	waitErr := wait.PollUntilContextTimeout(context.Background(), poll*5, pollTimeout*4, true,
		func(ctx context.Context) (bool, error) {
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
	err := invokeVCenterServiceControl(ctx, startOperation, service, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = waitVCenterServiceToBeInState(ctx, service, vcAddress, svcRunningMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*isSvcStopped = false
}

// assignPolicyToWcpNamespace assigns a set of storage policies to a wcp namespace
func assignPolicyToWcpNamespace(client clientset.Interface, ctx context.Context,
	namespace string, policyNames []string, resourceQuotaLimit string) {
	adminClient, _ := initializeClusterClientsByUserRoles(client)
	sessionId := createVcSession4RestApis(ctx)
	curlStr := ""
	policyNamesArrLength := len(policyNames)
	defRqLimit := strings.Split(resourceQuotaLimit, "Gi")[0]
	limit, err := strconv.Atoi(defRqLimit)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	limit *= 953 // to convert gb to mebibytes

	// Read hosts sshd port number
	vcIp, portNum, err := getPortNumAndIP(vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vcAddress := vcIp + ":" + portNum

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
		`'{"storage_specs": [ %s ]}'`, httpCodeStr, vcIp, namespace, sessionId, curlStr)

	framework.Logf("Running command: %s", curlCmd)
	result, err := fssh.SSH(ctx, curlCmd, vcAddress, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"couldn't execute command: %v due to err %v", curlCmd, err)
	}
	gomega.Expect(strconv.Atoi(result.Stdout)).To(gomega.Equal(status_code_success))

	// wait for sc to get created in SVC
	for _, policyName := range policyNames {
		err = waitForScToGetCreated(adminClient, ctx, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

}

// createVcSession4RestApis generates session ID for VC to use in rest API calls
func createVcSession4RestApis(ctx context.Context) string {
	nimbusGeneratedVcPwd := GetAndExpectStringEnvVar(vcUIPwd)
	// Read hosts sshd port number
	vcIp, portNum, err := getPortNumAndIP(vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := vcIp + ":" + portNum

	curlCmd := fmt.Sprintf("curl -k -X POST https://%s/rest/com/vmware/cis/session"+
		" -u 'Administrator@vsphere.local:%s'", vcIp, nimbusGeneratedVcPwd)
	framework.Logf("Running command: %s", curlCmd)
	result, err := fssh.SSH(ctx, curlCmd, addr, framework.TestContext.Provider)
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
	waitErr := wait.PollUntilContextTimeout(ctx, poll, pollTimeoutShort*5, true,
		func(ctx context.Context) (bool, error) {
			storageclass, err := client.StorageV1().StorageClasses().Get(ctx, policyName, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("couldn't find storageclass: %s due to error: %v", policyName, err)
			}
			if storageclass != nil {
				return true, nil
			}
			return false, nil
		})
	if wait.Interrupted(waitErr) {
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
	csiPods, err := fpod.GetPodsInNamespace(ctx, client, csiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var k8sMasterIP string
	if vanillaCluster {
		k8sMasterIPs := getK8sMasterIPs(ctx, client)
		k8sMasterIP = k8sMasterIPs[0]
	} else {
		k8sMasterIP = GetAndExpectStringEnvVar(svcMasterIP)
	}

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

// CheckDevice helps verify the raw block device inside pod is accessible correctly
func CheckDevice(ctx context.Context, client clientset.Interface, sts *appsv1.StatefulSet, devicePath string) error {
	for _, cmd := range []string{
		fmt.Sprintf("ls -idlh %v", devicePath),
		fmt.Sprintf("find %v", devicePath),
		fmt.Sprintf("dd if=/dev/zero of=%v bs=1024 count=1 seek=0", devicePath),
	} {
		if err := fss.ExecInStatefulPods(ctx, client, sts, cmd); err != nil {
			return fmt.Errorf("failed to check device in command %v, err %v", cmd, err)
		}
	}
	return nil
}

// verifyIOOnRawBlockVolume helps check data integrity for raw block volumes
func verifyIOOnRawBlockVolume(ns string, podName string, devicePath string, testdataFile string,
	startSizeInMB, dataSizeInMB int64) {
	// Write some data to file first and then to raw block device
	writeDataOnRawBlockVolume(ns, podName, devicePath, testdataFile, startSizeInMB, dataSizeInMB)
	// Read the data to verify that is it same as what written
	verifyDataFromRawBlockVolume(ns, podName, devicePath, testdataFile, startSizeInMB, dataSizeInMB)
}

// writeDataOnRawBlockVolume writes test data to raw block device
func writeDataOnRawBlockVolume(ns string, podName string, devicePath string, testdataFile string,
	startSizeInMB, dataSizeInMB int64) {
	cmd := []string{"exec", podName, "--namespace=" + ns, "--", "/bin/sh", "-c",
		fmt.Sprintf("/bin/ls %v", devicePath)}
	_, err := e2ekubectl.RunKubectl(ns, cmd...)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	_ = e2ekubectl.RunKubectlOrDie(ns, "cp", testdataFile, fmt.Sprintf(
		"%v/%v:/tmp/data_to_write", ns, podName))
	framework.ExpectNoError(err, fmt.Sprintf("failed to write testdata inside the pod: %q", podName))

	// If startSizeInMB is given, fill 1M with testData from offset=startSizeInMB given, upto dataSizeInMB.
	// Otherwise write the testData given from offset=0.
	gomega.Expect(dataSizeInMB).NotTo(gomega.BeZero())
	for i := int64(0); i < dataSizeInMB; i = i + 1 {
		seek := fmt.Sprintf("%v", startSizeInMB+i)
		cmd = []string{"exec", podName, "--namespace=" + ns, "--", "/bin/sh", "-c",
			fmt.Sprintf("/bin/dd if=/tmp/data_to_write of=%v bs=1M count=1 conv=fsync seek=%v",
				devicePath, seek)}
		_, err = e2ekubectl.RunKubectl(ns, cmd...)
		framework.ExpectNoError(err, fmt.Sprintf("failed to write device: %q inside the pod: %q", devicePath, podName))
	}
	cmd = []string{"--namespace=" + ns, "exec", podName, "--", "/bin/sh", "-c", "rm /tmp/data_to_write"}
	_ = e2ekubectl.RunKubectlOrDie(ns, cmd...)
}

// verifyDataFromRawBlockVolume reads data from raw block device and verifies it against given input
func verifyDataFromRawBlockVolume(ns string, podName string, devicePath string, testdataFile string,
	startSizeInMB, dataSizeInMB int64) {
	cmd := []string{"exec", podName, "--namespace=" + ns, "--", "/bin/sh", "-c",
		fmt.Sprintf("/bin/ls %v", devicePath)}
	_, err := e2ekubectl.RunKubectl(ns, cmd...)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Verify testData written on 1MB upto dataSizeInMB from the specified offset=startSizeInMB
	// Otherwise verify the testdata specified from offset=0
	gomega.Expect(dataSizeInMB).NotTo(gomega.BeZero())
	for i := int64(0); i < dataSizeInMB; i = i + 1 {
		skip := fmt.Sprintf("%v", startSizeInMB+i)
		cmd = []string{"exec", podName, "--namespace=" + ns, "--", "/bin/sh", "-c",
			fmt.Sprintf("/bin/dd if=%v of=/tmp/data_to_read bs=1M count=1 skip=%v", devicePath, skip)}
		_, err = e2ekubectl.RunKubectl(ns, cmd...)
		framework.ExpectNoError(err, fmt.Sprintf("failed to read device: %q inside the pod: %q", devicePath, podName))
		_ = e2ekubectl.RunKubectlOrDie(ns, "cp",
			fmt.Sprintf("%v/%v:/tmp/data_to_read", ns, podName), testdataFile+podName)

		framework.Logf("Running diff with source file and file from pod %v for 1M starting %vM", podName, skip)
		op, err := exec.Command("diff", testdataFile, testdataFile+podName).Output()
		framework.Logf("diff: %v", op)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(op)).To(gomega.BeZero())
	}

}

// getBlockDevSizeInBytes returns size of block device at given path
func getBlockDevSizeInBytes(f *framework.Framework, ns string, pod *v1.Pod, devicePath string) (int64, error) {
	cmd := []string{"exec", pod.Name, "--namespace=" + ns, "--", "/bin/sh", "-c",
		fmt.Sprintf("/bin/blockdev --getsize64 %v", devicePath)}
	output, err := e2ekubectl.RunKubectl(ns, cmd...)
	if err != nil {
		return -1, fmt.Errorf("failed to get size of raw device %v inside pod", devicePath)
	}
	output = strings.TrimSuffix(output, "\n")
	return strconv.ParseInt(output, 10, 64)
}

// checkClusterIdValueOnWorkloads checks clusterId value by querying cns metadata
// for all k8s workloads in a particular namespace
func checkClusterIdValueOnWorkloads(vs *vSphere, client clientset.Interface,
	ctx context.Context, namespace string, clusterID string) error {
	podList, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, pod := range podList.Items {
		pvcName := pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvName := pvc.Spec.VolumeName
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeID := pv.Spec.CSI.VolumeHandle
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
			if kubernetesMetadata.EntityType == "POD" && kubernetesMetadata.ClusterID != clusterID {
				return fmt.Errorf("clusterID %s is not matching with %s ", clusterID, kubernetesMetadata.ClusterID)
			} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME" &&
				kubernetesMetadata.ClusterID != clusterID {
				return fmt.Errorf("clusterID %s is not matching with %s ", clusterID, kubernetesMetadata.ClusterID)
			} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME_CLAIM" &&
				kubernetesMetadata.ClusterID != clusterID {
				return fmt.Errorf("clusterID %s is not matching with %s ", clusterID, kubernetesMetadata.ClusterID)
			}
		}
		framework.Logf("successfully verified clusterID of the volume %q", volumeID)
	}
	return nil
}

// Clean up statefulset and make sure no volume is left in CNS after it is deleted from k8s
// TODO: Code improvements is needed in case if the function is called from snapshot test.
// add a logic to delete the snapshots for the volumes and then delete volumes
func cleaupStatefulset(client clientset.Interface, ctx context.Context, namespace string,
	statefulset *appsv1.StatefulSet) {
	scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
	pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, claim := range pvcs.Items {
		pv := getPvFromClaim(client, namespace, claim.Name)
		err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
		err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll,
			pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeHandle := pv.Spec.CSI.VolumeHandle
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
				"kubernetes", volumeHandle))
	}
}

// createVsanDPvcAndPod is a wrapper method which creates vsand pvc and pod on svc master IP
func createVsanDPvcAndPod(sshClientConfig *ssh.ClientConfig, svcMasterIP string, svcNamespace string,
	pvcName string, podName string, storagePolicyName string, pvcSize string) error {
	err := applyVsanDirectPvcYaml(sshClientConfig, svcMasterIP, svcNamespace, pvcName, podName, storagePolicyName, pvcSize)
	if err != nil {
		return err
	}
	err = applyVsanDirectPodYaml(sshClientConfig, svcMasterIP, svcNamespace, pvcName, podName)
	if err != nil {
		return err
	}
	return nil
}

// applyVsanDirectPvcYaml creates specific pvc spec and applies vsan direct pvc  yaml on svc master
func applyVsanDirectPvcYaml(sshClientConfig *ssh.ClientConfig, svcMasterIP string, svcNamespace string,
	pvcName string, podName string, storagePolicyName string, pvcSize string) error {

	if pvcSize == "" {
		pvcSize = diskSize
	}
	cmd := fmt.Sprintf("sed -r"+
		" -i 's/^(\\s*)(namespace\\s*:\\s*.*\\s*$)/\\1namespace: %s/' pvc.yaml", svcNamespace)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err := sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = fmt.Sprintf("sed -r "+
		"-i 's/^(\\s*)(storageClassName\\s*:\\s*.*\\s*$)/\\1storageClassName: %s/' pvc.yaml", storagePolicyName)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = fmt.Sprintf("sed -r"+
		" -i 's/^(\\s*)(name\\s*:\\s*.*\\s*$)/\\1name: %s/' pvc.yaml", pvcName)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = fmt.Sprintf("sed -r"+
		" -i 's/^(\\s*)(storage\\s*:\\s*.*\\s*$)/\\1storage: %s/' pvc.yaml", pvcSize)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = fmt.Sprintf("sed -r -i "+
		"'s/^(\\s*)(volume\\.beta\\.kubernetes\\.io\\/storage-class\\s*:\\s*.*\\s*$)"+
		"/\\1volume\\.beta\\.kubernetes\\.io\\/storage-class: %s/' pvc.yaml", storagePolicyName)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = fmt.Sprintf("sed -r -i "+
		"'s/^(\\s*)(placement\\.beta\\.vmware\\.com\\/storagepool_antiAffinityRequired\\s*:\\s*.*\\s*$)"+
		"/\\1placement\\.beta\\.vmware\\.com\\/storagepool_antiAffinityRequired: %s/' pvc.yaml", podName)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = "kubectl apply -f pvc.yaml"
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}
	return nil
}

// applyVsanDirectPodYaml creates specific pod spec and applies vsan direct pod yaml on svc master
func applyVsanDirectPodYaml(sshClientConfig *ssh.ClientConfig, svcMasterIP string, svcNamespace string,
	pvcName string, podName string) error {
	framework.Logf("POD yaML")
	cmd := fmt.Sprintf("sed -i -e "+
		"'/^metadata:/,/namespace:/{/^\\([[:space:]]*namespace: \\).*/s//\\1%s/}' pod.yaml", svcNamespace)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err := sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = fmt.Sprintf("sed -i -e "+
		"'/^metadata:/,/claimName:/{/^\\([[:space:]]*claimName: \\).*/s//\\1%s/}' pod.yaml", pvcName)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = fmt.Sprintf("sed -i -e "+
		"'/^metadata:/,/name:/{/^\\([[:space:]]*name: \\).*/s//\\1%s/}' pod.yaml", podName)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = "kubectl apply -f pod.yaml"
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}
	return nil

}

// writeDataToMultipleFilesOnPodInParallel writes data to multiple files
// on a given pod in parallel
func writeDataToMultipleFilesOnPodInParallel(namespace string, podName string, data string,
	wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for i := 0; i < 10; i++ {
		ginkgo.By("write to a file in pod")
		filePath := fmt.Sprintf("/mnt/volume1/file%v.txt", i)
		writeDataOnFileFromPod(namespace, podName, filePath, data)
	}

}

// byFirstTimeStamp sorts a slice of events by first timestamp, using their involvedObject's name as a tie breaker.
type byFirstTimeStamp []v1.Event

func (o byFirstTimeStamp) Len() int      { return len(o) }
func (o byFirstTimeStamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func (o byFirstTimeStamp) Less(i, j int) bool {
	if o[i].FirstTimestamp.Equal(&o[j].FirstTimestamp) {
		return o[i].InvolvedObject.Name < o[j].InvolvedObject.Name
	}
	return o[i].FirstTimestamp.Before(&o[j].FirstTimestamp)
}

// dumpSvcNsEventsOnTestFailure dumps the events from the given namespace in case of test failure
func dumpSvcNsEventsOnTestFailure(client clientset.Interface, namespace string) {
	if !ginkgo.CurrentSpecReport().Failed() {
		return
	}
	dumpEventsInNs(client, namespace)
}

// dumpEventsInNs dumps events from the given namespace
func dumpEventsInNs(client clientset.Interface, namespace string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Found %d events in svc ns %s.", len(events.Items), namespace))
	sortedEvents := events.Items
	if len(sortedEvents) > 1 {
		sort.Sort(byFirstTimeStamp(sortedEvents))
	}
	for _, e := range sortedEvents {
		framework.Logf(
			"At %v - event for %v: %v %v: %v", e.FirstTimestamp, e.InvolvedObject.Name, e.Source, e.Reason, e.Message)
	}
}

// getAllPodsFromNamespace lists  and returns all pods from a given namespace
func getAllPodsFromNamespace(ctx context.Context, client clientset.Interface, namespace string) *v1.PodList {
	podList, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(podList).NotTo(gomega.BeNil())

	return podList
}

// getVmdkPathFromVolumeHandle returns VmdkPath associated with a given volumeHandle
// by running govc command
func getVmdkPathFromVolumeHandle(sshClientConfig *ssh.ClientConfig, masterIp string,
	datastoreName string, volHandle string) string {
	cmd := govcLoginCmd() + fmt.Sprintf("govc disk.ls -L=true -ds=%s -l  %s", datastoreName, volHandle)
	result, err := sshExec(sshClientConfig, masterIp, cmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	vmdkPath := result.Stdout
	return vmdkPath
}

// checkVcServicesHealthPostReboot returns waitErr, if VC services are not running in given timeout
func checkVcServicesHealthPostReboot(ctx context.Context, host string, timeout ...time.Duration) error {
	var pollTime time.Duration
	// if timeout is not passed then default pollTime to be set to 30 mins
	if len(timeout) == 0 {
		pollTime = pollTimeout * 6
	} else {
		pollTime = timeout[0]
	}
	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	//list of default stopped services in VC
	var defaultStoppedServicesList = []string{"vmcam", "vmware-imagebuilder", "vmware-netdumper",
		"vmware-perfcharts", "vmware-rbd-watchdog", "vmware-vcha"}
	waitErr := wait.PollUntilContextTimeout(ctx, pollTimeoutShort, pollTime, true,
		func(ctx context.Context) (bool, error) {
			var pendingServiceslist []string
			var noAdditionalServiceStopped = false
			sshCmd := fmt.Sprintf("service-control --%s", statusOperation)
			framework.Logf("Invoking command %v on vCenter host %v", sshCmd, addr)
			result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
			}
			framework.Logf("Command %v output is %v", sshCmd, result.Stdout)
			// getting list of services which are stopped currently
			services := strings.SplitAfter(result.Stdout, svcRunningMessage+":")
			servicesStoppedInVc := strings.Split(services[1], svcStoppedMessage+":")
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
						_ = invokeVCenterServiceControl(ctx, startOperation, service, host)
						// wait for 120 seconds,in case dependent service are still in pending state
						time.Sleep(120 * time.Second)
					}
				}
			}
			// Checking status for pendingStart Service list and starting it accordingly
			for _, service := range pendingServiceslist {
				framework.Logf("Checking status for additional service %s in StartPending state", service)
				sshCmd := fmt.Sprintf("service-control --%s %s", statusOperation, service)
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
					err = invokeVCenterServiceControl(ctx, startOperation, service, host)
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

// Fetches managed object reference for worker node VMs in the cluster
func getWorkerVmMoRefs(ctx context.Context, client clientset.Interface) []vim25types.ManagedObjectReference {
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	workervms := []vim25types.ManagedObjectReference{}
	for _, node := range nodes.Items {
		cpvm := false
		for label := range node.Labels {
			if label == controlPlaneLabel {
				cpvm = true
			}
		}
		if !cpvm {
			workervms = append(workervms, getHostMoref4K8sNode(ctx, client, &node))
		}
	}
	return workervms
}

// Returns a map of VM ips for kubernetes nodes and its moid
func vmIpToMoRefMap(ctx context.Context) map[string]vim25types.ManagedObjectReference {
	if vmIp2MoMap != nil {
		return vmIp2MoMap
	}
	vmIp2MoMap = make(map[string]vim25types.ManagedObjectReference)
	vmObjs := e2eVSphere.getAllVms(ctx)
	for _, mo := range vmObjs {
		if !strings.Contains(mo.Name(), "k8s") {
			continue
		}
		ip, err := mo.WaitForIP(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ip).NotTo(gomega.BeEmpty())
		vmIp2MoMap[ip] = mo.Reference()
		framework.Logf("VM with IP %s is named %s and its moid is %s", ip, mo.Name(), mo.Reference().Value)
	}
	return vmIp2MoMap

}

// Returns host MOID for a given k8s node VM by referencing node VM ip
func getHostMoref4K8sNode(
	ctx context.Context, client clientset.Interface, node *v1.Node) vim25types.ManagedObjectReference {
	vmIp2MoRefMap := vmIpToMoRefMap(ctx)
	return vmIp2MoRefMap[getK8sNodeIP(node)]
}

// set storagePolicyQuota
func setStoragePolicyQuota(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string, quota string) {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + storagePolicyQuota, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq.Spec.Limit.Reset()
	spq.Spec.Limit.Add(resource.MustParse(quota))
	framework.Logf("set quota %s", quota)

	err = cnsOperatorClient.Update(ctx, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// Remove storagePolicy Quota
func removeStoragePolicyQuota(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string) {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + storagePolicyQuota, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	increaseLimit := spq.Spec.Limit
	framework.Logf("Present quota Limit  %s", increaseLimit)
	spq.Spec.Limit.Reset()

	err = cnsOperatorClient.Update(ctx, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq = &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + storagePolicyQuota, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Quota after removing:  %s", spq.Spec.Limit)

}

// ToRef returns a pointer to t.
func ToRef[T any](t T) *T {
	return &t
}

// Deref returns the value referenced by t if not nil, otherwise the empty value
// for T is returned.
func Deref[T any](t *T) T {
	var empT T
	return DerefWithDefault(t, empT)
}

// DerefWithDefault returns the value referenced by t if not nil, otherwise
// defaulT is returned.
func DerefWithDefault[T any](t *T, defaulT T) T {
	if t != nil {
		return *t
	}
	return defaulT
}

// Get storagePolicyQuota consumption based on resourceType (i.e., either volume, snapshot, vmservice)
func getStoragePolicyQuotaForSpecificResourceType(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string, extensionType string, islatebinding bool) (*resource.Quantity, *resource.Quantity) {
	var usedQuota, reservedQuota *resource.Quantity
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + storagePolicyQuota, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	scLevelQuotaStatusList := spq.Status.SCLevelQuotaStatuses

	for _, item := range scLevelQuotaStatusList {
		expectedSCName := scName

		if islatebinding {
			expectedSCName += "-latebinding"
		}

		if item.StorageClassName != expectedSCName {
			continue
		}

		// Find the matching ResourceExtensionName
		for _, resItem := range spq.Status.ResourceTypeLevelQuotaStatuses {
			if resItem.ResourceExtensionName != extensionType {
				continue
			}
			// Choose the index based on late binding
			index := 0
			if islatebinding {
				index = 1
			}

			if len(resItem.ResourceTypeSCLevelQuotaStatuses) <= index {
				ginkgo.By(fmt.Sprintf("Quota status list index %d not available", index))
				break
			}

			usedQuota = resItem.ResourceTypeSCLevelQuotaStatuses[index].SCLevelQuotaUsage.Used
			reservedQuota = resItem.ResourceTypeSCLevelQuotaStatuses[index].SCLevelQuotaUsage.Reserved
			ginkgo.By(fmt.Sprintf("usedQuota: %v, reservedQuota: %v", usedQuota, reservedQuota))
			break
		}

		break // SC match found and processed; exit outer loop
	}

	return usedQuota, reservedQuota
}

// Get total quota consumption by storagePolicy
func getTotalQuotaConsumedByStoragePolicy(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string, islatebinding bool) (*resource.Quantity, *resource.Quantity) {

	var usedQuota, reservedQuota *resource.Quantity
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + storagePolicyQuota, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Determine expected StorageClass name
	expectedSCName := scName
	if islatebinding {
		expectedSCName += "-latebinding"
	}

	// Search for matching StorageClass entry
	for _, item := range spq.Status.SCLevelQuotaStatuses {
		if item.StorageClassName == expectedSCName {
			framework.Logf("**TotalQuotaConsumed for storage policy: %s **", expectedSCName)
			usedQuota = item.SCLevelQuotaUsage.Used
			reservedQuota = item.SCLevelQuotaUsage.Reserved
			ginkgo.By(fmt.Sprintf("usedQuota %v, reservedQuota %v", usedQuota, reservedQuota))
			break // exit after match
		}
	}
	return usedQuota, reservedQuota
}

// Get getStoragePolicyUsageForSpecificResourceType based on resourceType (i.e., either volume, snapshot, vmservice)
// resourceUsage will be either pvcUsage, vmUsage and snapshotUsage
func getStoragePolicyUsageForSpecificResourceType(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string, resourceUsage string) (*resource.Quantity, *resource.Quantity) {
	var usedQuota, reservedQuota *resource.Quantity
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	//spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	spq := &storagepolicyv1alpha2.StoragePolicyUsage{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + resourceUsage, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if spq.Status.ResourceTypeLevelQuotaUsage.DeepCopy() == nil {
		zeroQuantity := resource.MustParse("0")
		usedQuota = &zeroQuantity
		reservedQuota = &zeroQuantity
	} else {
		usedQuota = spq.Status.ResourceTypeLevelQuotaUsage.Used
		reservedQuota = spq.Status.ResourceTypeLevelQuotaUsage.Reserved
	}
	framework.Logf("** Storage_Policy_Usage for Resource : %s **", resourceUsage)
	framework.Logf("** Storage_Policy_Usage usedQuota : %s, reservedQuota:%s **", usedQuota, reservedQuota)
	return usedQuota, reservedQuota
}

func validate_totalStoragequota(ctx context.Context, diskSizes []string, totalUsedQuotaBefore *resource.Quantity,
	totalUsedQuotaAfter *resource.Quantity) bool {
	var validTotalQuota bool
	validTotalQuota = false
	var totalDiskStorage int64

	//Convert string in the Form "1Gi" to quotaBefore=1 and suffix=Gi or Mi
	out := make([]byte, 0, 64)
	result, diskUnit_quotaBefore := totalUsedQuotaBefore.CanonicalizeBytes(out)
	value := string(result)
	quotaBefore, err := strconv.ParseInt(string(value), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaBefore :  %v%s", quotaBefore, string(diskUnit_quotaBefore)))

	//Convert string in the Form "1Gi" to quotaAfter=1 and suffix=Gi or Mi
	result1, diskunit_quotaAfter := totalUsedQuotaAfter.CanonicalizeBytes(out)
	value1 := string(result1)
	quotaAfter, err := strconv.ParseInt(string(value1), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaAfter :  %v%s", quotaAfter, string(diskunit_quotaAfter)))

	totalDiskStorage = sumupAlltheResourceDiskUsage(diskSizes, diskunit_quotaAfter)

	//To calculate the ExpectedQuota, It is required to match the diskUnit's of
	//storageQuota that was showing before and after the workload creation
	if string(diskUnit_quotaBefore) != string(diskunit_quotaAfter) {
		if string(diskunit_quotaAfter) == "Mi" {
			if string(diskUnit_quotaBefore) == "Gi" {
				var bytes int64 = quotaBefore
				quotaBefore = int64(bytes) * 1024
			}
		}
		if string(diskunit_quotaAfter) == "Gi" {
			if string(diskUnit_quotaBefore) == "Mi" {
				var mb int64 = quotaBefore
				quotaBefore = mb / 1024
			}
		}
	}

	ginkgo.By(fmt.Sprintf("quotaBefore+diskSize:  %v, quotaAfter : %v",
		quotaBefore+totalDiskStorage, quotaAfter))
	ginkgo.By(fmt.Sprintf("totalDiskStorage:  %v", totalDiskStorage))

	if quotaBefore+totalDiskStorage == quotaAfter {
		validTotalQuota = true
		ginkgo.By(fmt.Sprintf("quotaBefore+diskSize:  %v, quotaAfter : %v",
			quotaBefore+totalDiskStorage, quotaAfter))
		ginkgo.By(fmt.Sprintf("validTotalQuota on storagePolicy:  %v", validTotalQuota))

	}
	return validTotalQuota
}

func validate_totalStoragequota_afterCleanUp(ctx context.Context, diskSize string,
	totalUsedQuotaBeforeCleanup *resource.Quantity, totalUsedQuotaAfterCleanup *resource.Quantity) bool {
	var validTotalQuota bool
	validTotalQuota = false

	out := make([]byte, 0, 64)
	result, diskUnit_quotaBefore := totalUsedQuotaBeforeCleanup.CanonicalizeBytes(out)
	value := string(result)
	quotaBeforeCleanUp, err := strconv.ParseInt(string(value), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaBefore :  %v%s", quotaBeforeCleanUp, string(diskUnit_quotaBefore)))

	result1, diskUnit_quotaAfter := totalUsedQuotaAfterCleanup.CanonicalizeBytes(out)
	value1 := string(result1)
	quotaAfter, err := strconv.ParseInt(string(value1), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf(" quotaAfter :  %v%s", quotaAfter, string(diskUnit_quotaAfter)))

	sizeStr := strings.TrimSpace(diskSize)
	ginkgo.By(fmt.Sprintf(" diskSize :  %s", diskSize))
	// Use regex to extract the numeric part
	rex := regexp.MustCompile(`\d+`)
	numStr := rex.FindString(sizeStr)

	// Convert to int64
	storage, err := strconv.ParseInt(numStr, 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Determine the unit
	diskSizeUnit := strings.ToUpper(strings.TrimPrefix(sizeStr, numStr))

	//To calculate the ExpectedQuota, It is required to match the diskUnit's of
	//storageQuota that was showing before and after the workload creation
	if string(diskUnit_quotaBefore) != string(diskUnit_quotaAfter) {
		if string(diskUnit_quotaAfter) == "Mi" && diskSizeUnit == "GI" {
			if string(diskUnit_quotaBefore) == "Gi" {
				var bytes int64 = quotaBeforeCleanUp
				quotaBeforeCleanUp = int64(bytes) * 1024
			}
			storage = storage * 1024
			fmt.Printf("Converted quotaBefore: %dMi and diskSiseUsed to Mi: %dMi\n", quotaBeforeCleanUp, storage)
		}
		if string(diskUnit_quotaAfter) == "Gi" && diskSizeUnit == "GI" {
			if string(diskUnit_quotaBefore) == "Mi" {
				var mb int64 = quotaBeforeCleanUp
				quotaBeforeCleanUp = mb / 1024
			}
			fmt.Printf("Converted quotaBefore: %dGi and diskSiseUsed to Gi: %dGi\n", quotaBeforeCleanUp, storage)
		}
	} else {
		if string(diskUnit_quotaAfter) == "Gi" && diskSizeUnit == "GI" {
			fmt.Printf(" quotaBefore: %dGi and diskSiseUsed to Gi: %dGi\n", quotaBeforeCleanUp, storage)
		}

		if string(diskUnit_quotaAfter) == "Mi" && diskSizeUnit == "GI" {
			storage = storage * 1024
			fmt.Printf("Converted quotaBefore: %dMi and diskSiseUsed to Mi: %dMi\n", quotaBeforeCleanUp, storage)
		}
	}

	quota := quotaBeforeCleanUp - storage
	fmt.Printf("cleanup quota : %v, quotaAfter: %v ", quota, quotaAfter)
	if quota == quotaAfter {
		validTotalQuota = true
		ginkgo.By(fmt.Sprintf("quotaBeforeCleanUp - diskSize: %v, quotaAfter : %v", quota, quotaAfter))
		ginkgo.By(fmt.Sprintf("validTotalQuota on storagePolicy:  %v", validTotalQuota))

	}

	return validTotalQuota
}

// validate_reservedQuota_afterCleanUp  after the volume goes to bound state or
// after teast clean up , expected reserved quota should be "0"
func validate_reservedQuota_afterCleanUp(ctx context.Context, total_reservedQuota *resource.Quantity,
	policy_reservedQuota *resource.Quantity, storagepolicyUsage_reserved_Quota *resource.Quantity) bool {
	ginkgo.By(fmt.Sprintf("reservedQuota on total storageQuota CR: %v"+
		"storagePolicyQuota CR: %v, storagePolicyUsage CR: %v ",
		total_reservedQuota.String(), policy_reservedQuota.String(), storagepolicyUsage_reserved_Quota.String()))

	//After the clean up it is expected to have reservedQuota to be '0'
	return total_reservedQuota.String() == "0" &&
		policy_reservedQuota.String() == "0" &&
		storagepolicyUsage_reserved_Quota.String() == "0"

}

func validate_increasedQuota(ctx context.Context, diskSize string, totalUsedQuotaBefore *resource.Quantity,
	totalUsedQuotaAfterexpansion *resource.Quantity) bool {
	var validTotalQuota bool
	validTotalQuota = false

	out := make([]byte, 0, 64)
	result, diskUnit_quotaBefore := totalUsedQuotaBefore.CanonicalizeBytes(out)
	value := string(result)
	quotaBefore, err := strconv.ParseInt(string(value), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaBefore :  %v%s", quotaBefore, string(diskUnit_quotaBefore)))

	result1, diskUnit_quotaAfter := totalUsedQuotaAfterexpansion.CanonicalizeBytes(out)
	value1 := string(result1)
	quotaAfter, err := strconv.ParseInt(string(value1), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf(" quotaAfter :  %v%s", quotaAfter, string(diskUnit_quotaAfter)))

	sizeStr := strings.TrimSpace(diskSize)
	ginkgo.By(fmt.Sprintf(" diskSize :  %s", diskSize))
	// Use regex to extract the numeric part
	rex := regexp.MustCompile(`\d+`)
	numStr := rex.FindString(sizeStr)

	// Convert to int64
	storage, err := strconv.ParseInt(numStr, 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Determine the unit
	diskSizeUnit := strings.ToUpper(strings.TrimPrefix(sizeStr, numStr))

	//To calculate the ExpectedQuota, It is required to match the diskUnit's of
	//storageQuota that was showing before and after the workload creation
	if string(diskUnit_quotaBefore) != string(diskUnit_quotaAfter) {
		if string(diskUnit_quotaAfter) == "Mi" && diskSizeUnit == "GI" {
			if string(diskUnit_quotaBefore) == "Gi" {
				var bytes int64 = quotaBefore
				quotaBefore = int64(bytes) * 1024
			}
			storage = storage * 1024
			fmt.Printf("Converted quotaBefore: %dMi and diskSiseUsed to Mi: %dMi\n", quotaBefore, storage)
		}
		if string(diskUnit_quotaAfter) == "Gi" && diskSizeUnit == "GI" {
			if string(diskUnit_quotaBefore) == "Mi" {
				var mb int64 = quotaBefore
				quotaBefore = mb / 1024
			}
			fmt.Printf("Converted quotaBefore: %dGi and diskSiseUsed to Gi: %dGi\n", quotaBefore, storage)
		}
	} else {
		if string(diskUnit_quotaAfter) == "Gi" && diskSizeUnit == "GI" {
			fmt.Printf(" quotaBefore: %dGi and diskSiseUsed to Gi: %dGi\n", quotaBefore, storage)
		}

		if string(diskUnit_quotaAfter) == "Mi" && diskSizeUnit == "GI" {
			storage = storage * 1024
			fmt.Printf("Converted quotaBefore: %dMi and diskSiseUsed to Mi: %dMi\n", quotaBefore, storage)
		}
	}

	if quotaBefore < quotaAfter {
		validTotalQuota = true
		ginkgo.By(fmt.Sprintf("quotaBefore +diskSize:  %v, quotaAfter : %v", quotaBefore, quotaAfter))
		ginkgo.By(fmt.Sprintf("validTotalQuota on storagePolicy:  %v", validTotalQuota))

	}
	return validTotalQuota
}

// StopKubeSystemPods function stops storageQuotaWebhook POD in kube-system namespace
func stopStorageQuotaWebhookPodInKubeSystem(ctx context.Context, client clientset.Interface,
	namespace string) (bool, error) {
	collectPodLogs(ctx, client, kubeSystemNamespace)
	isServiceStopped := false
	err := updateDeploymentReplicawithWait(client, 0, storageQuotaWebhookPrefix,
		namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = true
	return isServiceStopped, err
}

// startCSIPods function starts the csi pods and waits till all the pods comes up
func startStorageQuotaWebhookPodInKubeSystem(ctx context.Context, client clientset.Interface, csiReplicas int32,
	namespace string) (bool, error) {
	ignoreLabels := make(map[string]string)
	err := updateDeploymentReplicawithWait(client, csiReplicas, storageQuotaWebhookPrefix,
		namespace)
	if err != nil {
		return true, err
	}
	// Wait for the CSI Pods to be up and Running
	list_of_pods, err := fpod.GetPodsInNamespace(ctx, client, namespace, ignoreLabels)
	if err != nil {
		return true, err
	}
	num_csi_pods := len(list_of_pods)
	err = fpod.WaitForPodsRunningReady(ctx, client, namespace, int(num_csi_pods),
		pollTimeout)
	isServiceStopped := false
	return isServiceStopped, err
}

// getStoragePolicyUsedAndReservedQuotaDetails This method returns the used and reserved quota's from
// storageQuota CR, storagePolicyQuota CR and storagePolicyUsage CR
func getStoragePolicyUsedAndReservedQuotaDetails(ctx context.Context, restConfig *rest.Config,
	storagePolicyName string, namespace string, resourceUsageName string, resourceExtensionName string,
	islatebinding bool) (*resource.Quantity, *resource.Quantity,
	*resource.Quantity, *resource.Quantity, *resource.Quantity, *resource.Quantity) {

	var totalQuotaUsed, totalQuotaReserved, storagePolicyQuotaCRUsed, storagePolicyQuotaCRReserved *resource.Quantity
	var storagePolicyUsageCRUsed, storagePolicyUsageCRReserved *resource.Quantity

	totalQuotaUsed, totalQuotaReserved = getTotalQuotaConsumedByStoragePolicy(ctx, restConfig,
		storagePolicyName, namespace, islatebinding)
	framework.Logf("Namespace total-Quota-Used: %s,total-Quota-Reserved: %s ", totalQuotaUsed, totalQuotaReserved)

	if resourceExtensionName != "" {
		framework.Logf("**resourceUsageName: %s, resourceExtensionName: %s **", resourceUsageName,
			resourceExtensionName)
		storagePolicyQuotaCRUsed, storagePolicyQuotaCRReserved = getStoragePolicyQuotaForSpecificResourceType(ctx,
			restConfig, storagePolicyName, namespace, resourceExtensionName, islatebinding)
		framework.Logf("Policy-Quota-CR-Used: %s, Policy-Quota-CR-Reserved: %s", storagePolicyQuotaCRUsed,
			storagePolicyQuotaCRReserved)
	} else {
		storagePolicyQuotaCRUsed = nil
		storagePolicyQuotaCRReserved = nil
	}

	if resourceUsageName != "" {
		if islatebinding {
			storagePolicyUsageCRUsed, storagePolicyUsageCRReserved = getStoragePolicyUsageForSpecificResourceType(ctx,
				restConfig, storagePolicyName+"-latebinding", namespace, resourceUsageName)
		} else {
			storagePolicyUsageCRUsed, storagePolicyUsageCRReserved = getStoragePolicyUsageForSpecificResourceType(ctx,
				restConfig, storagePolicyName, namespace, resourceUsageName)
		}
		framework.Logf("Policy-Usage-CR-Used: %s, Policy-Usage-CR-Reserved: %s", storagePolicyUsageCRUsed,
			storagePolicyUsageCRReserved)
	} else {
		storagePolicyUsageCRUsed = nil
		storagePolicyUsageCRReserved = nil
	}

	return totalQuotaUsed, totalQuotaReserved,
		storagePolicyQuotaCRUsed, storagePolicyQuotaCRReserved,
		storagePolicyUsageCRUsed, storagePolicyUsageCRReserved
}

// validateQuotaUsageAfterResourceCreation Verifies the quota consumption after the resource creation
// resourceExtensionName :  volExtensionName / snapshotExtensionName / volExtensionName,
// resourceUsage: pvcUsage / snapshotUsage / vmUsage
func validateQuotaUsageAfterResourceCreation(ctx context.Context, restConfig *rest.Config, storagePolicyName string,
	namespace string, resourceUsage string, resourceExtensionName string, size []string,
	totalQuotaUsedBefore *resource.Quantity, storagePolicyQuotaBefore *resource.Quantity,
	storagePolicyUsageBefore *resource.Quantity, islatebinding bool) (bool, bool) {

	_, _, storagePolicyQuotaAfter, _, storagePolicyUsageAfter, _ :=
		getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
			storagePolicyName, namespace, resourceUsage, resourceExtensionName, islatebinding)

	sp_quota_validation := validate_totalStoragequota(ctx, size, storagePolicyQuotaBefore,
		storagePolicyQuotaAfter)
	framework.Logf("Storage-policy-Quota CR validation status :%v", sp_quota_validation)

	sp_usage_validation := validate_totalStoragequota(ctx, size, storagePolicyUsageBefore,
		storagePolicyUsageAfter)
	framework.Logf("Storage-policy-usage CR validation status :%v", sp_usage_validation)

	return sp_quota_validation, sp_usage_validation
}

// validateQuotaUsageAfterCleanUp verifies the storagequota details shows up on all CR's after resource cleanup
func validateQuotaUsageAfterCleanUp(ctx context.Context, restConfig *rest.Config, storagePolicyName string,
	namespace string, resourceUsage string, resourceExtensionName string, diskSizeInMb string,
	totalQuotaUsedAfter *resource.Quantity, storagePolicyQuotaAfter *resource.Quantity,
	storagePolicyUsageAfter *resource.Quantity, islatebinding bool) {

	totalQuotaUsedCleanup, totalQuotaReserved, storagePolicyQuotaAfterCleanup, storagePolicyQuotaReserved,
		storagePolicyUsageAfterCleanup, storagePolicyUsageReserved := getStoragePolicyUsedAndReservedQuotaDetails(
		ctx, restConfig, storagePolicyName, namespace, resourceUsage, resourceExtensionName, islatebinding)

	quotavalidationStatusAfterCleanup := validate_totalStoragequota_afterCleanUp(ctx, diskSizeInMb,
		totalQuotaUsedAfter, totalQuotaUsedCleanup)
	gomega.Expect(quotavalidationStatusAfterCleanup).NotTo(gomega.BeFalse())

	quotavalidationStatusAfterCleanup = validate_totalStoragequota_afterCleanUp(ctx, diskSizeInMb,
		storagePolicyQuotaAfter, storagePolicyQuotaAfterCleanup)
	gomega.Expect(quotavalidationStatusAfterCleanup).NotTo(gomega.BeFalse())

	quotavalidationStatusAfterCleanup = validate_totalStoragequota_afterCleanUp(ctx, diskSizeInMb,
		storagePolicyUsageAfter, storagePolicyUsageAfterCleanup)
	gomega.Expect(quotavalidationStatusAfterCleanup).NotTo(gomega.BeFalse())

	reservedQuota := validate_reservedQuota_afterCleanUp(ctx, totalQuotaReserved,
		storagePolicyQuotaReserved, storagePolicyUsageReserved)
	gomega.Expect(reservedQuota).NotTo(gomega.BeFalse())

	framework.Logf("quotavalidationStatus :%v reservedQuota:%v", quotavalidationStatusAfterCleanup,
		reservedQuota)
}

// execCommandOnGcWorker logs into gc worker node using ssh private key and executes command
func execCommandOnGcWorker(sshClientConfig *ssh.ClientConfig, svcMasterIP string, gcWorkerIp string,
	svcNamespace string, cmd string) (fssh.Result, error) {
	result := fssh.Result{Host: gcWorkerIp, Cmd: cmd}
	// get the cluster ssh key
	sshSecretName := GetAndExpectStringEnvVar(sshSecretName)
	cmdToGetPrivateKey := fmt.Sprintf("kubectl get secret %s -n %s -o"+
		"jsonpath={'.data.ssh-privatekey'} | base64 -d > key", sshSecretName, svcNamespace)
	framework.Logf("Invoking command '%v' on host %v", cmdToGetPrivateKey,
		svcMasterIP)
	cmdResult, err := sshExec(sshClientConfig, svcMasterIP,
		cmdToGetPrivateKey)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return result, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmdToGetPrivateKey, svcMasterIP, err)
	}

	enablePermissionCmd := "chmod 600 key"
	framework.Logf("Invoking command '%v' on host %v", enablePermissionCmd,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		enablePermissionCmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return result, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			enablePermissionCmd, svcMasterIP, err)
	}

	cmdToGetContainerInfo := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -i key %s@%s "+
		"'%s' 2> /dev/null", gcNodeUser, gcWorkerIp, cmd)
	framework.Logf("Invoking command '%v' on host %v", cmdToGetContainerInfo,
		svcMasterIP)
	cmdResult, err = sshExec(sshClientConfig, svcMasterIP,
		cmdToGetContainerInfo)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return result, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmdToGetContainerInfo, svcMasterIP, err)
	}
	return cmdResult, nil
}

// expandVolumeInParallel resizes a list of volumes to a new size in parallel
func expandVolumeInParallel(client clientset.Interface, pvclaims []*v1.PersistentVolumeClaim,
	wg *sync.WaitGroup, resizeValue string) {

	defer wg.Done()
	for _, pvclaim := range pvclaims {
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(resizeValue))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}
	}
}

/*
getVCversion returns the VC version
*/
func getVCversion(ctx context.Context, vcAddress string) string {
	if vcVersion == "" {
		// Read hosts sshd port number
		ip, portNum, err := getPortNumAndIP(vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		addr := ip + ":" + portNum

		sshCmd := "vpxd -v"
		framework.Logf("Checking if fss is enabled on vCenter host %v", addr)
		result, err := fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
		fssh.LogResult(result)
		if err == nil && result.Code == 0 {
			vcVersion = strings.TrimSpace(result.Stdout)
		} else {
			ginkgo.By(fmt.Sprintf("couldn't execute command: %s on vCenter host: %v", sshCmd, err))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// Regex to find version in the format X.Y.Z
		re := regexp.MustCompile(`\d+\.\d+\.\d+`)
		vcVersion = re.FindString(vcVersion)
	}
	framework.Logf("vcVersion %s", vcVersion)
	return vcVersion
}

/*
isVersionGreaterOrEqual returns true if presentVersion is equal to or greater than expectedVCversion
*/
func isVersionGreaterOrEqual(presentVersion, expectedVCversion string) bool {
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
Restart WCP with WaitGroup
*/
func restartWcpWithWg(ctx context.Context, vcAddress string, wg *sync.WaitGroup) {
	defer wg.Done()
	err := restartWcp(ctx, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

/*
Stop VC service with WaitGroup
*/
func stopServiceWithWg(ctx context.Context, vcAddress string, serviceName string, wg *sync.WaitGroup, c chan<- error) {
	var err error
	defer wg.Done()
	err = invokeVCenterServiceControl(ctx, stopOperation, serviceName, vcAddress)
	if err != nil {
		c <- err
	}
}

/*
Restart WCP
*/
func restartWcp(ctx context.Context, vcAddress string) error {
	err := invokeVCenterServiceControl(ctx, restartOperation, wcpServiceName, vcAddress)
	if err != nil {
		return fmt.Errorf("couldn't restart WCP: %v", err)
	}
	return nil
}

/*
Helper method to verify the status code
*/
func checkStatusCode(expectedStatusCode int, actualStatusCode int) error {
	gomega.Expect(actualStatusCode).Should(gomega.BeNumerically("==", expectedStatusCode))
	if actualStatusCode != expectedStatusCode {
		return fmt.Errorf("expected status code: %d, actual status code: %d", expectedStatusCode, actualStatusCode)
	}
	return nil
}

/*
This helper is to check if a given integer present in a list of intergers.
*/
func isAvailable(alpha []int, val int) bool {
	// iterate using the for loop
	for i := 0; i < len(alpha); i++ {
		// check
		if alpha[i] == val {
			// return true
			return true
		}
	}
	return false
}

/*
getPortNumAndIP function retrieves the SSHD port number for a given IP address,
considering whether the network is private or public.
*/
func getPortNumAndIP(ip string) (string, string, error) {
	port := "22"

	// Strip port if it's included in IP string
	if strings.Contains(ip, ":") {
		ip = strings.Split(ip, ":")[0]
	}

	// Check if running in private network
	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		localhost := GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)

		if p, exists := ipPortMap[ip]; exists {
			return localhost, p, nil
		}
		return ip, "", fmt.Errorf("port number is missing for IP: %s", ip)
	}

	return ip, port, nil
}

/*
createStaticVolumeOnSvc utility function to create a static volume on a service, register it with CNS,
and verify the PV/PVC setup
*/
func createStaticVolumeOnSvc(ctx context.Context, client clientset.Interface, namespace string, datastoreUrl string,
	storagePolicyName string) (string, *object.Datastore, *v1.PersistentVolumeClaim, *v1.PersistentVolume, error) {
	var datacenters []string
	var defaultDatacenter *object.Datacenter
	var pandoraSyncWaitTime int
	var defaultDatastore *object.Datastore
	curtime := time.Now().Unix()
	curtimestring := strconv.FormatInt(curtime, 10)
	pvcName := "cns-pvc-" + curtimestring

	// Get Kubernetes client configuration
	restConfig := getRestConfigClient()

	// Find and load datacenters
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("failed to get config: %v", err)
	}

	dcList := strings.Split(cfg.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}

	// Loop through datacenters to find the right one
	for _, dc := range datacenters {
		defaultDatacenter, err = finder.Datacenter(ctx, dc)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("failed to get datacenter '%s': %v", dc, err)
		}
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err = getDatastoreByURL(ctx, datastoreUrl, defaultDatacenter)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("failed to get datastore from "+
				"URL '%s' in datacenter '%s': %v", datastoreUrl, dc, err)
		}
	}

	// Get Storage Profile ID
	profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)

	// Create FCD (CNS Volume)
	fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
		"staticfcd"+curtimestring, profileID, diskSizeInMb, defaultDatastore.Reference())
	if err != nil {
		return "", defaultDatastore, nil, nil, fmt.Errorf("failed to create FCD with profile ID '%s': %v", profileID, err)
	}

	// Sync time for Pandora (if set in environment variable)
	if os.Getenv(envPandoraSyncWaitTime) != "" {
		pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
		if err != nil {
			return fcdID, defaultDatastore, nil, nil, fmt.Errorf("invalid Pandora sync "+
				"wait time '%s': %v", os.Getenv(envPandoraSyncWaitTime), err)
		}
	} else {
		pandoraSyncWaitTime = defaultPandoraSyncWaitTime
	}

	// Sleep to allow FCD to sync with Pandora
	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created "+
		"FCD:%s to sync with Pandora", pandoraSyncWaitTime, fcdID))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	// Register volume with CNS
	cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
	err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
	if err != nil {
		return fcdID, defaultDatastore, nil, nil,
			fmt.Errorf("failed to create CNS register volume for FCD '%s': %v", fcdID, err)
	}

	// Wait for CNS volume creation to complete
	framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx, restConfig,
		namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	framework.Logf("CNS register volume name: %s", cnsRegisterVolumeName)

	// Retrieve and verify PV and PVC
	ginkgo.By("Verifying created PV, PVC, and checking bidirectional reference")
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return fcdID, defaultDatastore, nil, nil,
			fmt.Errorf("failed to get PVC '%s' from namespace '%s': %v", pvcName, namespace, err)
	}

	pv := getPvFromClaim(client, namespace, pvcName)
	if pv == nil {
		return fcdID, defaultDatastore, pvc, nil,
			fmt.Errorf("failed to retrieve PV for PVC '%s' in namespace '%s'", pvcName, namespace)
	}

	verifyBidirectionalReferenceOfPVandPVC(ctx, client, pvc, pv, fcdID)

	return fcdID, defaultDatastore, pvc, pv, nil
}

/*
This util will fetch and compare the storage policy usage CR created for each storage class for a namespace
*/
func ListStoragePolicyUsages(ctx context.Context, c clientset.Interface, restClientConfig *rest.Config,
	namespace string, storageclass []string) {

	// Build expected usage names directly from passed storageclass names
	expectedUsages := make(map[string]bool)
	for _, sc := range storageclass {
		for _, suffix := range usageSuffixes {
			expectedUsages[fmt.Sprintf("%s%s", sc, suffix)] = false
		}
	}

	if len(expectedUsages) == 0 {
		fmt.Println("Error: No storage class names provided.")
		return
	}

	// CNS Operator client
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		fmt.Printf("Error: Failed to create CNS operator client: %v\n", err)
		return
	}

	// Poll until all expected usages are found
	waitErr := wait.PollUntilContextTimeout(ctx, storagePolicyUsagePollInterval, storagePolicyUsagePollTimeout,
		true, func(ctx context.Context) (bool, error) {
			spuList := &storagepolicyv1alpha2.StoragePolicyUsageList{}
			err := cnsOperatorClient.List(ctx, spuList, &client.ListOptions{Namespace: namespace})
			if err != nil {
				return false, nil
			}

			// Reset all expected usages to false
			for k := range expectedUsages {
				expectedUsages[k] = false
			}

			// Mark found usages
			for _, spu := range spuList.Items {
				if _, exists := expectedUsages[spu.Name]; exists {
					expectedUsages[spu.Name] = true
				}
			}

			// Check if all usages have been found
			for _, found := range expectedUsages {
				if !found {
					return false, nil
				}
			}

			return true, nil
		})

	// Collect both found and missing usages
	var found, missing []string
	for name, isFound := range expectedUsages {
		if isFound {
			found = append(found, name)
		} else {
			missing = append(missing, name)
		}
	}

	fmt.Println("Storage Policy Usage Summary:")
	fmt.Printf("Found:   %v\n", found)
	if len(missing) > 0 {
		fmt.Printf("Missing: %v\n", missing)
		fmt.Printf("Error: Timed out waiting for all storage policy usages: %v\n", waitErr)
		return
	}

	fmt.Println("All required storage policy usages are available.")
}

func reconfigPolicyParallel(ctx context.Context, volID string, policyId string, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	err := e2eVSphere.reconfigPolicy(ctx, volID, policyId)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// when PVC is deleted, used following method to check for PV status
func waitForPvToBeReleased(ctx context.Context, client clientset.Interface,
	pvName string) (*v1.PersistentVolume, error) {
	var pv *v1.PersistentVolume
	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, pollTimeoutShort, true,
		func(ctx context.Context) (bool, error) {
			pv, err = client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if pv.Status.Phase == v1.VolumeReleased {
				return true, nil
			}
			return false, nil
		})
	return pv, waitErr
}

// convertGiStrToMibInt64 returns integer numbers of Mb equivalent to string
// of the form \d+Gi.
func convertGiStrToMibInt64(size resource.Quantity) int64 {
	r, err := regexp.Compile("[0-9]+")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	sizeInt, err := strconv.Atoi(r.FindString(size.String()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return int64(sizeInt * 1024)
}

// This method gets storageclass and creates resource quota
// Returns restConfig,  storageclass and profileId
// This is used in staticProvisioning for presetup.
func staticProvisioningPreSetUpUtil(ctx context.Context, f *framework.Framework,
	c clientset.Interface, storagePolicyName string) (*rest.Config, *storagev1.StorageClass, string) {
	namespace := getNamespaceToRunTests(f)
	adminClient, _ := initializeClusterClientsByUserRoles(c)
	// Get a config to talk to the apiserver
	k8senv := GetAndExpectStringEnvVar("KUBECONFIG")
	restConfig, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
	framework.Logf("Profile ID :%s", profileID)
	scParameters := make(map[string]string)
	scParameters["storagePolicyID"] = profileID

	if !supervisorCluster {
		err = adminClient.StorageV1().StorageClasses().Delete(ctx, storagePolicyName, metav1.DeleteOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

	storageclass, err := createStorageClass(c, scParameters, nil, "", "", true, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("storageclass Name: %s", storageclass.GetName()))
	storageclass, err = adminClient.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("create resource quota")
	createResourceQuota(adminClient, namespace, rqLimit, storagePolicyName)

	return restConfig, storageclass, profileID
}

// Convert mb to String
func convertInt64ToStrMbFormat(diskSize int64) string {
	result := strconv.FormatInt(diskSize, 10) + "Mi"
	fmt.Println(result)
	return result
}

// Convert gb to String
func convertInt64ToStrGbFormat(diskSize int64) string {
	result := strconv.FormatInt(diskSize, 10) + "Gi"
	fmt.Println(result)
	return result
}

// Convert all the disks storage unit to the expected unit value and Sums up and returns the totalDiskStorage
func sumupAlltheResourceDiskUsage(diskSizes []string, expectedUnit []byte) int64 {
	var totalDiskStorage int64
	var diskUnit string

	fmt.Println("Length of slice:", len(diskSizes))

	for _, diskSize := range diskSizes {
		sizeStr := strings.TrimSpace(diskSize)
		ginkgo.By(fmt.Sprintf("diskSize :  %s", diskSize))
		ginkgo.By(fmt.Sprintf("present totalDiskStorage :  %d", totalDiskStorage))
		// Use regex to extract the numeric part
		rex := regexp.MustCompile(`\d+`)
		numStr := rex.FindString(sizeStr)

		// Convert to int64
		storage, err := strconv.ParseInt(numStr, 10, 64)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("storage : %d", storage)

		// Determine the unit
		//diskUnit = strings.ToUpper(strings.TrimPrefix(sizeStr, numStr))
		diskUnit = strings.TrimPrefix(sizeStr, numStr)
		framework.Logf("diskUnit : %s", diskUnit)

		if string(expectedUnit) == "Gi" {
			if diskUnit == "Gi" {
				totalDiskStorage = totalDiskStorage + storage
			} else {
				storageGi := storage / 1024
				totalDiskStorage = totalDiskStorage + storageGi
			}
		} else if string(expectedUnit) == "Mi" {
			if diskUnit == "Mi" {
				totalDiskStorage = totalDiskStorage + storage
			} else {
				storageMi := storage * 1024
				totalDiskStorage = totalDiskStorage + storageMi
			}
		}
	}

	framework.Logf("sum of all the disk storages = %d ", totalDiskStorage)
	return totalDiskStorage

}

// Validate Total quota of all resources
// size: will have array of disk size from PVC, snapshot's and vmserviceVm
func validateTotalQuota(ctx context.Context, restConfig *rest.Config, storagePolicyName string,
	namespace string, size []string,
	totalQuotaUsedBefore *resource.Quantity, islatebinding bool) (*resource.Quantity, bool) {

	totalQuotaUsedAfter, _, _, _, _, _ :=
		getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
			storagePolicyName, namespace, "", "", islatebinding)

	quotavalidationStatus := validate_totalStoragequota(ctx, size, totalQuotaUsedBefore,
		totalQuotaUsedAfter)
	framework.Logf("totalStoragequota validation status :%v", quotavalidationStatus)

	return totalQuotaUsedAfter, quotavalidationStatus
}

/*
This function creates a wcp namespace in a vSphere supervisor Cluster, associating it
with multiple storage policies and zones.
It constructs an API request and sends it to the vSphere REST API.
*/
func createtWcpNsWithZonesAndPolicies(
	vcRestSessionId string, storagePolicyId []string,
	supervisorId string, zoneNames []string,
	vmClass string, contentLibId string) (string, int, error) {

	r := rand.New(rand.NewSource(time.Now().Unix()))
	namespace := fmt.Sprintf("csi-vmsvcns-%v", r.Intn(10000))
	initailUrl := createInitialNsApiCallUrl()
	nsCreationUrl := initailUrl + "v2"

	var storageSpecs []map[string]string
	for _, policyId := range storagePolicyId {
		storageSpecs = append(storageSpecs, map[string]string{"policy": policyId})
	}

	var zones []map[string]string
	for _, zone := range zoneNames {
		zones = append(zones, map[string]string{"name": zone})
	}

	// Create request body struct
	requestBody := map[string]interface{}{
		"namespace":     namespace,
		"storage_specs": storageSpecs,
		"supervisor":    supervisorId,
		"zones":         zones,
	}

	// Add vm_service_spec only if vmClass and contentLibId are provided
	if vmClass != "" && contentLibId != "" {
		requestBody["vm_service_spec"] = map[string]interface{}{
			"vm_classes":        []string{vmClass},
			"content_libraries": []string{contentLibId},
		}
	}

	reqBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return "", 500, fmt.Errorf("error marshalling request body: %w", err)
	}

	reqBody := string(reqBodyBytes)
	fmt.Println(reqBody)

	// Make the API request
	_, statusCode := invokeVCRestAPIPostRequest(vcRestSessionId, nsCreationUrl, reqBody)

	return namespace, statusCode, nil
}

// Generates random string of requested length
func genrateRandomString(length int) (string, error) {
	var generatedString string
	rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length+2)
	_, err := cryptoRand.Read(b)
	if err != nil {
		return generatedString, fmt.Errorf("error marshalling request body: %w", err)
	}
	generatedString = fmt.Sprintf("%x", b)[2 : length+2]
	return generatedString, err
}

// WaitForPVClaimBoundPhase waits until all pvcs phase set to bound
// client: framework generated client
// pvclaims: list of PVCs
// timeout: timeInterval to wait for PVCs to get into bound state
// ctx: context package variable
func WaitForPVClaimBoundPhase(ctx context.Context, client clientset.Interface,
	pvclaims []*v1.PersistentVolumeClaim, timeout time.Duration) ([]*v1.PersistentVolume, error) {
	persistentvolumes := make([]*v1.PersistentVolume, len(pvclaims))

	adminClient, client := initializeClusterClientsByUserRoles(client)
	for index, claim := range pvclaims {
		err := fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
			claim.Namespace, claim.Name, framework.Poll, timeout)
		if err != nil {
			return persistentvolumes, err
		}
		// Get new copy of the claim
		claim, err = client.CoreV1().PersistentVolumeClaims(claim.Namespace).
			Get(ctx, claim.Name, metav1.GetOptions{})
		if err != nil {
			return persistentvolumes, fmt.Errorf("PVC Get API error: %w", err)
		}
		// Get the bounded PV
		persistentvolumes[index], err = adminClient.CoreV1().PersistentVolumes().
			Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return persistentvolumes, fmt.Errorf("PV Get API error: %w", err)
		}
	}
	return persistentvolumes, nil
}

// createScopedClient generates a kubernetes-client by constructing kubeconfig by
// creating a user with minimal permissions and enable port forwarding. It takes
// client: framework generated client
// namespace: framework generated namespace name
// saName: service account name
// ctx: context package variable
func createScopedClient(ctx context.Context, client clientset.Interface,
	ns string, saName string) (clientset.Interface, error) {

	roleName := ns + "role"
	roleBindingName := roleName + "-binding"
	contextName := "e2e-context"

	_, err := client.CoreV1().ServiceAccounts(ns).Create(ctx, &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: saName,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create SA: %v", err)
	}

	// 2. Create Role
	_, err = client.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name: roleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "persistentvolumeclaims", "services"},
				Verbs:     []string{"get", "watch", "list", "delete", "create", "update"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"statefulsets", "deployments", "replicasets"},
				Verbs:     []string{"get", "watch", "list", "delete", "create", "update"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"events"},
				Verbs:     []string{"get", "list"},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create Role: %v", err)
	}

	time.Sleep(5 * time.Second)

	_, err = client.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: roleBindingName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      serviceAccountKeyword,
				Name:      saName,
				Namespace: ns,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     roleKeyword,
			Name:     roleName,
			APIGroup: rbacApiGroup,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create RoleBinding: %v", err)
	}

	var token string

	tr := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{audienceForSvcAccountName},
		},
	}

	tokenRequest, err := client.CoreV1().ServiceAccounts(ns).CreateToken(ctx, saName, tr, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %v", err)
	}
	token = tokenRequest.Status.Token

	if token == "" {
		return nil, fmt.Errorf("no token found for service account")
	}
	time.Sleep(60 * time.Second)
	localPort := GetAndExpectStringEnvVar("RANDOM_PORT")
	framework.Logf("Random port: %s", localPort)
	kubeConfig := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"e2e-cluster": {
				Server:                fmt.Sprintf("https://127.0.0.1:%s", localPort),
				InsecureSkipTLSVerify: true,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			contextName: {
				Cluster:   "e2e-cluster",
				AuthInfo:  "e2e-user",
				Namespace: ns,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"e2e-user": {
				Token: token,
			},
		},
		CurrentContext: contextName,
	}

	restCfg, err := clientcmd.NewNonInteractiveClientConfig(kubeConfig,
		contextName, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build rest.Config: %v", err)
	}

	nsScopedClient, err := clientset.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Clientset: %v", err)
	}
	return nsScopedClient, nil
}

// initializeClusterClientsByUserRoles takes a client generated by test framework
// and generates and returns an admin client with administrator priviledges
// and a client with devops user priviledges
func initializeClusterClientsByUserRoles(client clientset.Interface) (clientset.Interface, clientset.Interface) {
	var adminClient clientset.Interface
	var err error
	runningAsDevopsUser := GetBoolEnvVarOrDefault(envIsDevopsUser, false)
	if supervisorCluster || guestCluster {
		if runningAsDevopsUser {
			if svAdminK8sEnv := GetAndExpectStringEnvVar(envAdminKubeconfig); svAdminK8sEnv != "" {
				adminClient, err = createKubernetesClientFromConfig(svAdminK8sEnv)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if supervisorCluster {
				if devopsK8sEnv := GetAndExpectStringEnvVar(envDevopsKubeconfig); devopsK8sEnv != "" {
					client, err = createKubernetesClientFromConfig(devopsK8sEnv)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		} else {
			adminClient = client
		}
	} else if vanillaCluster || adminClient == nil {
		adminClient = client
	}
	return adminClient, client
}

// getSvcConfigSecretData returns data obtained fom csi config secret
// in namespace where CSI is deployed
func getSvcConfigSecretData(client clientset.Interface, ctx context.Context,
	csiNamespace string) (e2eTestConfig, error) {
	var vsphereCfg e2eTestConfig
	currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
	if err != nil {
		return vsphereCfg, err
	}
	originalConf := string(currentSecret.Data[vsphereCloudProviderConfiguration])
	vsphereCfg, err = readConfigFromSecretString(originalConf)
	if err != nil {
		return vsphereCfg, err
	}

	return vsphereCfg, nil
}
