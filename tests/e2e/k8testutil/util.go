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

package k8testutil

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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanmethods "github.com/vmware/govmomi/vsan/methods"
	vsantypes "github.com/vmware/govmomi/vsan/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/vsan"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"

	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var (
	svcClient              clientset.Interface
	svcClient1             clientset.Interface
	svcNamespace           string
	svcNamespace1          string
	vsanHealthClient       *vsan.VsanClient
	clusterComputeResource []*object.ClusterComputeResource
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

// scaleDownNDeleteStsDeploymentsInNamespace scales down and deletes all statefulsets and deployments in given namespace
func ScaleDownNDeleteStsDeploymentsInNamespace(ctx context.Context, c clientset.Interface, ns string) {
	ssList, err := c.AppsV1().StatefulSets(ns).List(
		ctx, metav1.ListOptions{LabelSelector: labels.Everything().String()})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, sts := range ssList.Items {
		ss := &sts
		if ss, err = fss.Scale(ctx, c, ss, 0); err != nil {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		fss.WaitForStatusReplicas(ctx, c, ss, 0)
		framework.Logf("Deleting statefulset %v", ss.Name)
		// Use OrphanDependents=false so it's deleted synchronously.
		// We already made sure the Pods are gone inside Scale().
		deletePolicy := metav1.DeletePropagationForeground
		err = c.AppsV1().StatefulSets(ns).Delete(ctx, ss.Name, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	depList, err := c.AppsV1().Deployments(ns).List(
		ctx, metav1.ListOptions{LabelSelector: labels.Everything().String()})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, deployment := range depList.Items {
		dep := &deployment
		err = UpdateDeploymentReplicawithWait(c, 0, dep.Name, ns)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deletePolicy := metav1.DeletePropagationForeground
		err = c.AppsV1().Deployments(ns).Delete(ctx, dep.Name, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return
			} else {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}
	}
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

// getPvFromClaim returns PersistentVolume for requested claim.
func GetPvFromClaim(client clientset.Interface, namespace string, claimName string) *v1.PersistentVolume {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pv
}

// getVirtualDeviceByDiskID gets the virtual device by diskID.
func GetVirtualDeviceByDiskID(ctx context.Context, vm *object.VirtualMachine,
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
func GetPersistentVolumeClaimSpecWithStorageClass(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	disksize := constants.DiskSize
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
func GetPersistentVolumeClaimSpecWithoutStorageClass(namespace string, ds string,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	disksize := constants.DiskSize
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
func CreatePVCAndStorageClass(ctx context.Context, vs *config.E2eTestConfig,
	client clientset.Interface, pvcnamespace string,
	pvclaimlabels map[string]string, scParameters map[string]string, ds string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode,
	names ...string) (*storagev1.StorageClass, *v1.PersistentVolumeClaim, error) {
	scName := ""
	if len(names) > 0 {
		scName = names[0]
	}
	storageclass, err := CreateStorageClass(client, vs, scParameters,
		allowedTopologies, "", bindingMode, allowVolumeExpansion, scName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvclaim, err := CreatePVC(ctx, client, pvcnamespace, pvclaimlabels, ds, storageclass, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return storageclass, pvclaim, err
}

// Clean up statefulset and make sure no volume is left in CNS after it is deleted from k8s
// TODO: Code improvements is needed in case if the function is called from snapshot test.
// add a logic to delete the snapshots for the volumes and then delete volumes
func CleaupStatefulset(vs *config.E2eTestConfig, client clientset.Interface, ctx context.Context, namespace string,
	statefulset *appsv1.StatefulSet) {
	ScaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
	pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, claim := range pvcs.Items {
		pv := GetPvFromClaim(client, namespace, claim.Name)
		err := fpv.DeletePersistentVolumeClaim(ctx, client, claim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
		err = fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, constants.Poll,
			constants.PollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volumeHandle := pv.Spec.CSI.VolumeHandle
		err = vcutil.WaitForCNSVolumeToBeDeleted(vs, volumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
				"kubernetes", volumeHandle))
	}
}

// getVSphereStorageClassSpec returns Storage Class Spec with supplied storage
// class parameters.
func GetVSphereStorageClassSpec(vs *config.E2eTestConfig, scName string, scParameters map[string]string,
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
		Provisioner:          constants.E2evSphereCSIDriverName,
		VolumeBindingMode:    &bindingMode,
		AllowVolumeExpansion: &allowVolumeExpansion,
	}
	// If scName is specified, use that name, else auto-generate storage class
	// name.

	if scName != "" {
		if vs.TestInput.ClusterFlavor.SupervisorCluster {
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

// waitForVolumeDetachedFromNode checks volume is detached from the node
// This function checks disks status every 3 seconds until detachTimeout, which is set to 360 seconds
func WaitForVolumeDetachedFromNode(client clientset.Interface,
	volumeID string, nodeName string, vs *config.E2eTestConfig) (bool, error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if vs.TestInput.ClusterFlavor.SupervisorCluster {
		_, err := vcutil.GetVMByUUIDWithWait(ctx, vs, nodeName, constants.SupervisorClusterOperationsTimeout)
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
				vmUUID, _ = vcutil.GetVMUUIDFromNodeName(vs, nodeName)
			}
			diskAttached, err := vcutil.IsVolumeAttachedToVM(client, vs, volumeID, vmUUID)
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

// psodHostWithPv methods finds the esx host where pv is residing and psods it.
// It uses VsanObjIndentities and QueryVsanObjects apis to achieve it and
// returns the host ip.
func PsodHostWithPv(ctx context.Context, vs *config.E2eTestConfig, pvName string) string {
	ginkgo.By("VsanObjIndentities")
	framework.Logf("pvName %v", pvName)

	vsanObjuuid := vcutil.VsanObjIndentities(ctx, vs, pvName)
	framework.Logf("vsanObjuuid %v", vsanObjuuid)
	gomega.Expect(vsanObjuuid).NotTo(gomega.BeNil())

	ginkgo.By("Get host info using queryVsanObj")
	hostInfo := vcutil.QueryVsanObj(ctx, vs, vsanObjuuid)
	framework.Logf("vsan object ID %v", hostInfo)
	gomega.Expect(hostInfo).NotTo(gomega.BeEmpty())
	hostIP := vcutil.GetHostUUID(ctx, vs, hostInfo)
	framework.Logf("hostIP %v", hostIP)
	gomega.Expect(hostIP).NotTo(gomega.BeEmpty())

	err := PsodHost(vs, hostIP, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return hostIP
}

/* This util will perform psod operation on a host */
func PsodHost(vs *config.E2eTestConfig, hostIP string, psodTimeOut string) error {
	ginkgo.By("PSOD")
	var timeout string
	if psodTimeOut != "" {
		timeout = psodTimeOut
	} else {
		timeout = constants.PsodTime
	}
	sshCmd := fmt.Sprintf("vsish -e set /config/Misc/intOpts/BlueScreenTimeout %s", timeout)
	op, err := RunCommandOnESX(vs, constants.RootUser, hostIP, sshCmd)
	framework.Logf("%q", op)
	if err != nil {
		return fmt.Errorf("failed to set BlueScreenTimeout: %w", err)
	}

	ginkgo.By("Injecting PSOD")
	psodCmd := "vsish -e set /reliability/crashMe/Panic 1; exit"
	op, err = RunCommandOnESX(vs, constants.RootUser, hostIP, psodCmd)
	framework.Logf("%q", op)
	if err != nil {
		return fmt.Errorf("failed to inject PSOD: %w", err)
	}
	return nil
}

// hostLogin methods sets the ESX host password.
func HostLogin(user, instruction string, questions []string, echos []bool) (answers []string, err error) {
	answers = make([]string, len(questions))
	nimbusGeneratedEsxPwd := env.GetAndExpectStringEnvVar(constants.NimbusEsxPwd)
	for n := range questions {
		answers[n] = nimbusGeneratedEsxPwd
	}
	return answers, nil
}

// createStorageClass helps creates a storage class with specified name,
// storageclass parameters.
func CreateStorageClass(client clientset.Interface, vs *config.E2eTestConfig, scParameters map[string]string,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	scReclaimPolicy v1.PersistentVolumeReclaimPolicy, bindingMode storagev1.VolumeBindingMode,
	allowVolumeExpansion bool, scName string) (*storagev1.StorageClass, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var storageclass *storagev1.StorageClass
	var err error
	isStorageClassPresent := false
	p := map[string]string{}

	if scParameters == nil && os.Getenv(constants.EnvHciMountRemoteDs) == "true" {
		p[constants.ScParamStoragePolicyName] = os.Getenv(constants.EnvStoragePolicyNameForHCIRemoteDatastores)
		scParameters = p
	}
	ginkgo.By(fmt.Sprintf("Creating StorageClass %s with scParameters: %+v and allowedTopologies: %+v "+
		"and ReclaimPolicy: %+v and allowVolumeExpansion: %t",
		scName, scParameters, allowedTopologies, scReclaimPolicy, allowVolumeExpansion))

	if vs.TestInput.ClusterFlavor.SupervisorCluster {
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if storageclass != nil && err == nil {
			isStorageClassPresent = true
		}
	}

	if !isStorageClassPresent {
		storageclass, err = client.StorageV1().StorageClasses().Create(ctx, GetVSphereStorageClassSpec(vs, scName,
			scParameters, allowedTopologies, scReclaimPolicy, bindingMode, allowVolumeExpansion), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create storage class with err: %v", err))
	}

	return storageclass, err
}

// updateSCtoDefault updates the given SC to default
func UpdateSCtoDefault(ctx context.Context, client clientset.Interface, scName, isDefault string) error {

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
func CreatePVC(ctx context.Context, client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string,
	ds string, storageclass *storagev1.StorageClass,
	accessMode v1.PersistentVolumeAccessMode) (*v1.PersistentVolumeClaim, error) {
	pvcspec := GetPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s "+
		"and labels: %+v accessMode: %+v", storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := fpv.CreatePVC(ctx, client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
	framework.Logf("PVC created: %v in namespace: %v", pvclaim.Name, pvcnamespace)
	return pvclaim, err
}

// createPVC helps creates pvc with given namespace and labels using given
// storage class.
func ScaleCreatePVC(ctx context.Context, client clientset.Interface, pvcnamespace string,
	pvclaimlabels map[string]string, ds string, storageclass *storagev1.StorageClass,
	accessMode v1.PersistentVolumeAccessMode, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()

	pvcspec := GetPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
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
func ScaleCreateDeletePVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string,
	ds string, storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode,
	wg *sync.WaitGroup, lock *sync.Mutex, worker int) {
	defer ginkgo.GinkgoRecover()
	ctx, cancel := context.WithCancel(context.Background())
	var totalPVCDeleted = 0
	defer cancel()
	defer wg.Done()
	for index := 1; index <= worker; index++ {
		pvcspec := GetPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
		ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v",
			storageclass.Name, ds, pvclaimlabels, accessMode))
		pvclaim, err := fpv.CreatePVC(ctx, client, pvcnamespace, pvcspec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))

		lock.Lock()
		pvclaims = append(pvclaims, pvclaim)
		pvclaimToDelete := RandomPickPVC()
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
func RandomPickPVC() *v1.PersistentVolumeClaim {
	index := rand.Intn(len(pvclaims))
	pvclaims[len(pvclaims)-1], pvclaims[index] = pvclaims[index], pvclaims[len(pvclaims)-1]
	pvclaimToDelete := pvclaims[len(pvclaims)-1]
	pvclaims = pvclaims[:len(pvclaims)-1]
	framework.Logf("pvc to delete %v", pvclaimToDelete.Name)
	return pvclaimToDelete
}

// createStatefulSetWithOneReplica helps create a stateful set with one replica.
func CreateStatefulSetWithOneReplica(client clientset.Interface, manifestPath string,
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

// UpdateDeploymentReplicawithWait helps to update the replica for a deployment
// with wait.
func UpdateDeploymentReplicawithWait(client clientset.Interface,
	count int32, name string, namespace string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deployment *appsv1.Deployment
	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, constants.HealthStatusPollInterval,
		constants.HealthStatusPollTimeout, true,
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
func UpdateDeploymentReplica(client clientset.Interface,
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
// Default namespace used here is constants.CsiSystemNamespace.
func BringDownCsiController(Client clientset.Interface, namespace ...string) {
	if len(namespace) == 0 {
		UpdateDeploymentReplica(Client, 0, constants.VSphereCSIControllerPodNamePrefix, constants.CsiSystemNamespace)
	} else {
		UpdateDeploymentReplica(Client, 0, constants.VSphereCSIControllerPodNamePrefix, namespace[0])
	}
	ginkgo.By("Controller is down")
}

// bringDownTKGController helps to bring the TKG control manager pod down.
// Its taks svc client as input.
func BringDownTKGController(Client clientset.Interface, vsphereTKGSystemNamespace string) {
	UpdateDeploymentReplica(Client, 0, constants.VsphereControllerManager, vsphereTKGSystemNamespace)
	ginkgo.By("TKGControllManager replica is set to 0")
}

// bringUpCsiController helps to bring the csi controller pod down.
// Default namespace used here is constants.CsiSystemNamespace.
func BringUpCsiController(Client clientset.Interface, csiReplicaCount int32, namespace ...string) {
	if len(namespace) == 0 {
		err := UpdateDeploymentReplicawithWait(Client, csiReplicaCount,
			constants.VSphereCSIControllerPodNamePrefix, constants.CsiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		err := UpdateDeploymentReplicawithWait(Client, csiReplicaCount,
			constants.VSphereCSIControllerPodNamePrefix, namespace[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	ginkgo.By("Controller is up")
}

// bringUpTKGController helps to bring the TKG control manager pod up.
// Its taks svc client as input.
func BringUpTKGController(Client clientset.Interface, tkgReplica int32, vsphereTKGSystemNamespace string) {
	err := UpdateDeploymentReplicawithWait(Client,
		tkgReplica, constants.VsphereControllerManager, vsphereTKGSystemNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("TKGControllManager is up")
}

func GetSvcClientAndNamespace() (clientset.Interface, string) {
	var err error
	if svcClient == nil {
		if k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
			svcClient, err = CreateKubernetesClientFromConfig(k8senv)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		svcNamespace = env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace)
	}
	return svcClient, svcNamespace
}

// create kubernetes client for multi-supervisors clusters and returns it along with namespaces
func GetMultiSvcClientAndNamespace() ([]clientset.Interface, []string, []error) {
	var err error
	if svcClient == nil {
		if k8senv := env.GetAndExpectStringEnvVar("KUBECONFIG"); k8senv != "" {
			svcClient, err = CreateKubernetesClientFromConfig(k8senv)
			if err != nil {
				return []clientset.Interface{}, []string{}, []error{err, nil}
			}
		}
		svcNamespace = env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace)
	}
	if svcClient1 == nil {
		if k8senv := env.GetAndExpectStringEnvVar("KUBECONFIG1"); k8senv != "" {
			svcClient1, err = CreateKubernetesClientFromConfig(k8senv)
			if err != nil {
				return []clientset.Interface{}, []string{}, []error{nil, err}
			}
		}
		svcNamespace1 = env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace1)
	}
	// returns list of clientset, namespace and error if any for both svc
	return []clientset.Interface{svcClient, svcClient1},
		[]string{svcNamespace, svcNamespace1}, []error{}
}

// updateCSIDeploymentTemplateFullSyncInterval helps to update the
// FULL_SYNC_INTERVAL_MINUTES in deployment template. For this to take effect,
// we need to terminate the running csi controller pod.
// Returns fsync interval value before the change.
func UpdateCSIDeploymentTemplateFullSyncInterval(client clientset.Interface,
	mins string, namespace string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deployment, err := client.AppsV1().Deployments(namespace).Get(
		ctx, constants.VSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
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
	_, err = client.AppsV1().Deployments(namespace).Update(ctx,
		deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Polling for update operation on deployment to take effect...")
	waitErr := wait.PollUntilContextTimeout(ctx, constants.HealthStatusPollInterval,
		constants.HealthStatusPollTimeout, true,
		func(ctx context.Context) (bool, error) {
			deployment, err = client.AppsV1().Deployments(namespace).Get(
				ctx, constants.VSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
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
func UpdateCSIDeploymentProvisionerTimeout(client clientset.Interface,
	namespace string, timeout string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ignoreLabels := make(map[string]string)
	list_of_pods, err := fpod.GetPodsInNamespace(ctx, client,
		constants.CsiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	num_csi_pods := len(list_of_pods)

	deployment, err := client.AppsV1().Deployments(namespace).Get(
		ctx, constants.VSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
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
	_, err = client.AppsV1().Deployments(namespace).Update(ctx,
		deployment, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Waiting for a min for update operation on deployment to take effect...")
	time.Sleep(1 * time.Minute)
	err = fpod.WaitForPodsRunningReady(ctx, client,
		constants.CsiSystemNamespace, int(num_csi_pods),
		time.Duration(2*constants.PollTimeout))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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

// GetDatastoreByURL returns the *Datastore instance given its URL.
func GetDatastoreByURL(ctx context.Context,
	datastoreURL string, dc *object.Datacenter) (*object.Datastore, error) {
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
func GetPersistentVolumeClaimSpec(namespace string,
	labels map[string]string, pvName string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := "shared-ds-policy"
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
func GetPersistentVolumeSpec(fcdID string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
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
				Driver:       constants.E2evSphereCSIDriverName,
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
			StorageClassName: "shared-ds-policy",
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = constants.E2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// Create PVC spec with given namespace, labels and pvName.
func GetPersistentVolumeClaimSpecForRWX(namespace string, labels map[string]string,
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
func GetPersistentVolumeSpecForRWX(fcdID string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
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
				Driver:       constants.E2evSphereCSIDriverName,
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
	annotations["pv.kubernetes.io/provisioned-by"] = constants.E2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// HttpRequest takes client and http Request as input and performs GET operation
// and returns bodybytes
func HttpRequest(client *http.Client, req *http.Request) ([]byte, int) {
	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("API Response status %d", resp.StatusCode)

	return bodyBytes, resp.StatusCode

}

// getVMImages returns the available gc images present in svc
func GetVMImages(wcpHost string, wcpToken string) VMImages {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	var vmImage VMImages
	client := &http.Client{Transport: transCfg}
	getVirtualMachineImagesURL := "https://" + wcpHost + constants.VmOperatorAPI + "virtualmachineimages"
	framework.Logf("URL %v", getVirtualMachineImagesURL)
	wcpToken = "Bearer " + wcpToken
	req, err := http.NewRequest("GET", getVirtualMachineImagesURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	bodyBytes, statusCode := HttpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	err = json.Unmarshal(bodyBytes, &vmImage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return vmImage
}

// deleteTKG method deletes the TKG Cluster
func DeleteTKG(wcpHost string, wcpToken string, tkgCluster string) error {
	ginkgo.By("Delete TKG")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + constants.TkgAPI + tkgCluster
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken

	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	_, statusCode := HttpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	req, err = http.NewRequest("DELETE", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	_, statusCode = HttpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	// get gc and validate if gc is deleted
	req, err = http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)

	err = WaitForDeleteToComplete(client, req)

	return err

}

// WaitForDeleteToComplete method constants.Polls for the requested object status
// returns true if its deleted successfully else returns error
func WaitForDeleteToComplete(client *http.Client, req *http.Request) error {
	waitErr := wait.PollUntilContextTimeout(context.Background(),
		constants.PollTimeoutShort, constants.PollTimeout*6, true,
		func(ctx context.Context) (bool, error) {
			framework.Logf("Polling for New GC status")
			_, statusCode := HttpRequest(client, req)

			if statusCode == 404 {
				framework.Logf("requested object is deleted")
				return true, nil
			}
			return false, nil
		})
	return waitErr
}

// upgradeTKG method updates the TKG Cluster with the tkgImage
func UpgradeTKG(wcpHost string, wcpToken string, tkgCluster string, tkgImage string) {
	ginkgo.By("Upgrade TKG")
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + constants.TkgAPI + tkgCluster
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken

	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	bodyBytes, statusCode := HttpRequest(client, req)
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

// CreateGC method creates GC and takes WCP host and bearer token as input param
func CreateGC(wcpHost string, wcpToken string, tkgImageName string, clusterName string) {

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	createGCURL := "https://" + wcpHost + constants.TkgAPI
	framework.Logf("URL %v", createGCURL)
	tkg_yaml, err := filepath.Abs(constants.GcManifestPath + "tkg.yaml")
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
	bodyBytes, statusCode := HttpRequest(client, req)

	response := string(bodyBytes)
	framework.Logf("%q", response)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 201))
}

func GetAuthToken(refreshToken string) string {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	urlValues := url.Values{}
	urlValues.Set("refresh_token", refreshToken)

	client := &http.Client{Transport: transCfg}
	req, err := http.NewRequest("POST", constants.AuthAPI, strings.NewReader(urlValues.Encode()))
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
func RotateVCCertinVMC(authToken string, orgId string, sddcId string) string {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	client := &http.Client{Transport: transCfg}
	certRotateURL := constants.VmcPrdEndpoint + orgId + "/sddcs/" + sddcId + "/certificate/VCENTER"

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

// getTaskStatus constants.Polls status for given task id and returns true once task is completed
func GetTaskStatus(authToken string, orgID string, taskID string) error {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	client := &http.Client{Transport: transCfg}
	getTaskStatusURL := constants.VmcPrdEndpoint + orgID + "/tasks/" + taskID
	framework.Logf("getTaskStatusURL %s", getTaskStatusURL)

	req, err := http.NewRequest("GET", getTaskStatusURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	waitErr := wait.PollUntilContextTimeout(context.Background(),
		constants.PollTimeoutShort, constants.PollTimeoutShort*5, true,
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
func ScaleTKGWorker(wcpHost string, wcpToken string, tkgCluster string, tkgworker int) {

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + constants.TkgAPI + tkgCluster
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken
	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)
	bodyBytes, statusCode := HttpRequest(client, req)
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
	bodyBytes, statusCode = HttpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	response := string(bodyBytes)
	framework.Logf("response %v", response)

}

// getGC polls for the GC status, returns error if its not in running phase
func GetGC(wcpHost string, wcpToken string, gcName string) error {
	var response string
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	getGCURL := "https://" + wcpHost + constants.TkgAPI + gcName
	framework.Logf("URL %v", getGCURL)
	wcpToken = "Bearer " + wcpToken
	req, err := http.NewRequest("GET", getGCURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("Authorization", wcpToken)

	waitErr := wait.PollUntilContextTimeout(context.Background(),
		constants.PollTimeoutShort, constants.PollTimeout*6, true,
		func(ctx context.Context) (bool, error) {
			framework.Logf("Polling for New GC status")
			bodyBytes, statusCode := HttpRequest(client, req)
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
func GetWCPSessionId(hostname string, username string, password string) string {
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
	bodyBytes, statusCode := HttpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var wcpSessionID WcpSessionID
	err = json.Unmarshal(bodyBytes, &wcpSessionID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("SessionID: %s", wcpSessionID.Session_id)

	return wcpSessionID.Session_id

}

// getWindowsFileSystemSize finds the windowsWorkerIp and returns the size of the volume
func GetWindowsFileSystemSize(client clientset.Interface, vs *config.E2eTestConfig, pod *v1.Pod) (int64, error) {
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
			windowsWorkerIP = GetK8sNodeIP(&node)
			break
		}
	}
	cmd := "Get-Disk | Format-List -Property Manufacturer,Size"
	if vs.TestInput.ClusterFlavor.GuestCluster {
		svcMasterIp := env.GetAndExpectStringEnvVar(constants.SvcMasterIP)
		svcMasterPwd := env.GetAndExpectStringEnvVar(constants.SvcMasterPassword)
		svcNamespace = env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace)
		sshWcpConfig := &ssh.ClientConfig{
			User: constants.RootUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		output, err = ExecCommandOnGcWorker(sshWcpConfig, vs, svcMasterIp, windowsWorkerIP,
			svcNamespace, cmd)
	} else {
		nimbusGeneratedWindowsVmPwd := env.GetAndExpectStringEnvVar(constants.EnvWindowsPwd)
		windowsUser := env.GetAndExpectStringEnvVar(constants.EnvWindowsUser)
		sshClientConfig := &ssh.ClientConfig{
			User: windowsUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedWindowsVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		output, err = SshExec(sshClientConfig, vs, windowsWorkerIP, cmd)
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
			if vs.TestInput.ClusterFlavor.GuestCluster {
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

// GetSvcConfigSecretData returns data obtained fom csi config secret
// in namespace where CSI is deployed
func GetSvcConfigSecretData(client clientset.Interface, ctx context.Context,
	csiNamespace string) (config.E2eTestConfig, error) {
	var vsphereCfg config.E2eTestConfig
	currentSecret, err := client.CoreV1().Secrets(csiNamespace).Get(ctx,
		constants.ConfigSecret, metav1.GetOptions{})
	if err != nil {
		return vsphereCfg, err
	}
	originalConf := string(currentSecret.Data[constants.VsphereCloudProviderConfiguration])
	vsphereCfg, err = ReadConfigFromSecretString(originalConf)
	if err != nil {
		return vsphereCfg, err
	}

	return vsphereCfg, nil
}

// performPasswordRotationOnSupervisor invokes the given command to replace the password
//
//	Vmon-cli is used to restart the wcp service after changing the time.
func PerformPasswordRotationOnSupervisor(client clientset.Interface, ctx context.Context,
	csiNamespace string, host string, vs *config.E2eTestConfig) (bool, error) {

	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	host = ip + ":" + portNum

	// getting supervisorID and password
	vsphereCfg, err := GetSvcConfigSecretData(client, ctx, csiNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	oldPassword := vsphereCfg.TestInput.Global.Password
	// stopping wcp service
	sshCmd := fmt.Sprintf("vmon-cli --stop %s", constants.WcpServiceName)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
	time.Sleep(constants.SleepTimeOut)
	if err != nil || result.Code != 0 {
		return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	// To set last_storage_pwd_rotation_timestamp with 12 hrs prior to current timestamp
	currentTimestamp := time.Now()
	twelveHoursAgoTimestamp := (currentTimestamp.Add(-12 * time.Hour)).Unix()
	sshCmd = fmt.Sprintf("cd /etc/vmware/wcp; sudo -u wcp psql -U wcpuser -d VCDB -c "+
		"\"update vcenter_svc_accounts set last_pwd_rotation_timestamp=%v "+
		"where instance_id='%s'\"", twelveHoursAgoTimestamp,
		vsphereCfg.TestInput.Global.SupervisorID)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err = fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}

	// Starting wcp service
	sshCmd = fmt.Sprintf("vmon-cli --start %s", constants.WcpServiceName)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err = fssh.SSH(ctx, sshCmd, host, framework.TestContext.Provider)
	time.Sleep(constants.SleepTimeOut)
	if err != nil || result.Code != 0 {
		return false, fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	// waiting for the changed password to reflect in CSI config file
	waitErr := wait.PollUntilContextTimeout(ctx,
		constants.PollTimeoutShort, constants.PwdRotationTimeout, true,
		func(ctx context.Context) (bool, error) {
			vsphereCfg, err := GetSvcConfigSecretData(client, ctx, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			changedPassword := vsphereCfg.TestInput.Global.Password
			framework.Logf("Password from Config Secret of svc: %v", changedPassword)
			if changedPassword != oldPassword {
				return true, nil
			}
			return false, nil
		})
	return true, waitErr
}

// GetVCentreSessionId gets vcenter session id to work with vcenter apis
func GetVCentreSessionId(hostname string, username string, password string) string {

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

	bodyBytes, statusCode := HttpRequest(client, req)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 201))

	var sessionID string
	err = json.Unmarshal(bodyBytes, &sessionID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("SessionID: %s", sessionID)

	return sessionID
}

// GetWCPCluster get the wcp cluster details
func GetWCPCluster(sessionID string, hostIP string) string {

	type WCPCluster struct {
		Cluster string
	}

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{Transport: transCfg}
	clusterURL := "https://" + hostIP + constants.VcClusterAPI
	framework.Logf("URL %v", clusterURL)

	req, err := http.NewRequest("GET", clusterURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req.Header.Add("vmware-api-session-id", sessionID)

	bodyBytes, statusCode := HttpRequest(client, req)

	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var wcpCluster []WCPCluster
	err = json.Unmarshal(bodyBytes, &wcpCluster)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("wcp cluster %+v\n", wcpCluster[0].Cluster)

	return wcpCluster[0].Cluster
}

// GetWCPHost gets the wcp host details
func GetWCPHost(wcpCluster string, hostIP string, sessionID string) string {
	type WCPClusterInfo struct {
		Api_server_management_endpoint string `json:"api_server_management_endpoint"`
	}
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	client := &http.Client{Transport: transCfg}
	clusterURL := "https://" + hostIP + constants.VcClusterAPI + "/" + wcpCluster
	framework.Logf("URL %v", clusterURL)

	req, err := http.NewRequest("GET", clusterURL, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add("vmware-api-session-id", sessionID)
	bodyBytes, statusCode := HttpRequest(client, req)
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

// VerifyVolumeTopology verifies that the Node Affinity rules in the volume.
// Match the topology constraints specified in the storage class.
func VerifyVolumeTopology(pv *v1.PersistentVolume,
	zoneValues []string, regionValues []string) (string, string, error) {
	if pv.Spec.NodeAffinity == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return "", "", fmt.Errorf("node Affinity rules for PV should exist in topology aware provisioning")
	}
	var pvZone string
	var pvRegion string
	for _, labels := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions {
		if labels.Key == constants.ZoneKey {
			for _, value := range labels.Values {
				gomega.Expect(zoneValues).To(gomega.ContainElement(value),
					fmt.Sprintf("Node Affinity rules for PV %s: %v does not contain zone specified in storage class %v",
						pv.Name, value, zoneValues))
				pvZone = value
			}
		}
		if labels.Key == constants.RegionKey {
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

// VerifyPodLocation verifies that a pod is scheduled on a node that belongs
// to the topology on which PV is provisioned.
func VerifyPodLocation(pod *v1.Pod,
	nodeList *v1.NodeList, zoneValue string, regionValue string) error {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			for labelKey, labelValue := range node.Labels {
				if labelKey == constants.ZoneKey && zoneValue != "" {
					gomega.Expect(zoneValue).To(gomega.Equal(labelValue),
						fmt.Sprintf("Pod %s is not running on Node located in zone %v", pod.Name, zoneValue))
				}
				if labelKey == constants.RegionKey && regionValue != "" {
					gomega.Expect(regionValue).To(gomega.Equal(labelValue),
						fmt.Sprintf("Pod %s is not running on Node located in region %v", pod.Name, regionValue))
				}
			}
		}
	}
	return nil
}

// GetTopologyFromPod rturn topology value from node affinity information.
func GetTopologyFromPod(pod *v1.Pod, nodeList *v1.NodeList) (string, string, error) {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			podRegion := node.Labels[constants.RegionKey]
			podZone := node.Labels[constants.ZoneKey]
			return podRegion, podZone, nil
		}
	}
	err := errors.New("could not find the topology from pod")
	return "", "", err
}

// TopologyParameterForStorageClass creates a topology map using the topology
// values ENV variables. Returns the allowedTopologies parameters required for
// the Storage Class.
// Input : <region-1>:<zone-1>, <region-1>:<zone-2>
// Output : [region-1], [zone-1, zone-2] {region-1: zone-1, region-1:zone-2}
func TopologyParameterForStorageClass(topology string) ([]string,
	[]string, []v1.TopologySelectorLabelRequirement) {
	topologyMap := CreateTopologyMap(topology)
	regionValues, zoneValues := GetValidTopology(topologyMap)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{
		{
			Key:    constants.RegionKey,
			Values: regionValues,
		},
		{
			Key:    constants.ZoneKey,
			Values: zoneValues,
		},
	}
	return regionValues, zoneValues, allowedTopologies
}

// CreateTopologyMap strips the topology string provided in the environment
// variable into a map from region to zones.
// example envTopology = "r1:z1,r1:z2,r2:z3"
func CreateTopologyMap(topologyString string) map[string][]string {
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

// GetValidTopology returns the regions and zones from the input topology map
// so that they can be provided to a storage class.
func GetValidTopology(topologyMap map[string][]string) ([]string, []string) {
	var regionValues []string
	var zoneValues []string
	for region, zones := range topologyMap {
		regionValues = append(regionValues, region)
		zoneValues = append(zoneValues, zones...)
	}
	return regionValues, zoneValues
}

// CreateResourceQuota creates resource quota for the specified namespace.
func CreateResourceQuota(client clientset.Interface,
	vs *config.E2eTestConfig, namespace string, size string, scName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var storagePolicyNameForSharedDatastores string
	var storagePolicyNameForSvc1 string
	var storagePolicyNameForSvc2 string

	waitTime := 15
	var executeCreateResourceQuota bool
	executeCreateResourceQuota = true
	if !vs.TestInput.TestBedInfo.MultipleSvc {
		// reading export variable from a single supervisor cluster
		storagePolicyNameForSharedDatastores = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDatastores)
	} else {
		// reading multiple export variables from a multi svc setup
		storagePolicyNameForSvc1 = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDsSvc1)
		storagePolicyNameForSvc2 = env.GetAndExpectStringEnvVar(constants.EnvStoragePolicyNameForSharedDsSvc2)
	}

	if vs.TestInput.ClusterFlavor.SupervisorCluster {
		_, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, namespace+"-storagequota", metav1.GetOptions{})
		if !vs.TestInput.TestBedInfo.MultipleSvc {
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
		DeleteResourceQuota(client, namespace)

		resourceQuota := NewTestResourceQuota(constants.QuotaName, size, scName)
		resourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Create(ctx, resourceQuota, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Create Resource quota: %+v", resourceQuota))
		ginkgo.By(fmt.Sprintf("Waiting for %v seconds to allow resourceQuota to be claimed", waitTime))
		time.Sleep(time.Duration(waitTime) * time.Second)
	}
}

// DeleteResourceQuota deletes resource quota for the specified namespace,
// if it exists.
func DeleteResourceQuota(client clientset.Interface, namespace string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, constants.QuotaName, metav1.GetOptions{})
	if err == nil {
		err = client.CoreV1().ResourceQuotas(namespace).Delete(ctx, constants.QuotaName, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Deleted Resource quota: %+v", constants.QuotaName))
	}
}

// checks if resource quota gets updated or not
func CheckResourceQuota(client clientset.Interface, namespace string, name string, size string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeoutShort, true,
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
func SetResourceQuota(client clientset.Interface, namespace string, size string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// deleteResourceQuota if already present.
	DeleteResourceQuota(client, namespace)
	existingResourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Get(ctx, namespace, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("existingResourceQuota name %s and requested size %v", existingResourceQuota.GetName(), size)
		requestStorageQuota := UpdatedSpec4ExistingResourceQuota(existingResourceQuota.GetName(), size)
		testResourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Update(
			ctx, requestStorageQuota, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("ResourceQuota details: %+v", testResourceQuota))
		err = CheckResourceQuota(client, namespace, existingResourceQuota.GetName(), size)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// NewTestResourceQuota returns a quota that enforces default constraints for
// testing.
func NewTestResourceQuota(name string, size string, scName string) *v1.ResourceQuota {
	hard := v1.ResourceList{}
	// Test quota on discovered resource type.
	hard[v1.ResourceName(scName+constants.RqStorageType)] = resource.MustParse(size)
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: hard},
	}
}

// updatedSpec4ExistingResourceQuota returns a quota that enforces default
// constraints for testing.
func UpdatedSpec4ExistingResourceQuota(name string, size string) *v1.ResourceQuota {
	updateQuota := v1.ResourceList{}
	// Test quota on discovered resource type.
	updateQuota[v1.ResourceName("requests.storage")] = resource.MustParse(size)
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: updateQuota},
	}
}

// CheckEventsforError prints the list of all events that occurred in the
// namespace and searches for expectedErrorMsg among these events.
func CheckEventsforError(client clientset.Interface, namespace string,
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

// GetPVCFromSupervisorCluster takes name of the persistentVolumeClaim as input,
// returns the corresponding persistentVolumeClaim object.
func GetPVCFromSupervisorCluster(pvcName string) *v1.PersistentVolumeClaim {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svcClient, svcNamespace := GetSvcClientAndNamespace()
	pvc, err := svcClient.CoreV1().PersistentVolumeClaims(svcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvc).NotTo(gomega.BeNil())
	return pvc
}

// GetVolumeIDFromSupervisorCluster returns SV PV volume handle for given SVC
// PVC name.
func GetVolumeIDFromSupervisorCluster(pvcName string) string {
	var svcClient clientset.Interface
	var err error
	if k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = CreateKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace)
	svcPV := GetPvFromClaim(svcClient, svNamespace, pvcName)
	volumeHandle := svcPV.Spec.CSI.VolumeHandle
	ginkgo.By(fmt.Sprintf("Found volume in Supervisor cluster with VolumeID: %s", volumeHandle))

	return volumeHandle
}

// getPvFromSupervisorCluster returns SV PV for given SVC PVC name.
func GetPvFromSupervisorCluster(pvcName string) *v1.PersistentVolume {
	var svcClient clientset.Interface
	var err error
	if k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = CreateKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := env.GetAndExpectStringEnvVar(constants.EnvSupervisorClusterNamespace)
	svcPV := GetPvFromClaim(svcClient, svNamespace, pvcName)
	return svcPV
}

// verfiy File exists or not in vSphere Volume
// TODO : add logic to disable disk caching in windows
func VerifyFilesExistOnVSphereVolume(e2eTestConfig *config.E2eTestConfig,
	namespace string, podName string, poll, timeout time.Duration,
	filePaths ...string) {
	var err error
	for _, filePath := range filePaths {
		if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
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
func VerifyFsTypeOnVsphereVolume(e2eTestConfig *config.E2eTestConfig,
	namespace string, podName string, expectedContent string, filePaths ...string) {
	var err error
	for _, filePath := range filePaths {
		if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
			filePath = filePath + ".txt"
			// TODO : add logic to disable disk caching in windows
			VerifyFilesExistOnVSphereVolume(e2eTestConfig,
				namespace, podName, constants.Poll, constants.PollTimeoutShort, filePath)
			_, err = e2eoutput.LookForStringInPodExec(namespace, podName,
				[]string{"powershell.exe", "cat", filePath}, constants.NtfsFSType, time.Minute)
		} else {
			_, err = e2eoutput.LookForStringInPodExec(namespace, podName,
				[]string{"/bin/cat", filePath}, expectedContent, time.Minute)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func CreateEmptyFilesOnVSphereVolume(e2eTestConfig *config.E2eTestConfig,
	namespace string, podName string, filePaths []string) {
	var err error
	for _, filePath := range filePaths {
		if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
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
	svcManifestFilePath := filepath.Join("..", constants.ManifestPath, "service.yaml")
	framework.Logf("Parsing service from %v", svcManifestFilePath)
	svc, err := manifest.SvcFromManifest(svcManifestFilePath)
	framework.ExpectNoError(err)

	service, err := c.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return service
}

// DeleteService deletes a k8s service.
func DeleteService(ns string, c clientset.Interface, service *v1.Service) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.CoreV1().Services(ns).Delete(ctx, service.Name, *metav1.NewDeleteOptions(0))
	framework.ExpectNoError(err)
}

// GetStatefulSetFromManifest creates a StatefulSet from the statefulset.yaml
// file present in the manifest path.
func GetStatefulSetFromManifest(e2eTestConfig *config.TestInputData, ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join("..", constants.ManifestPath, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	if e2eTestConfig.TestBedInfo.WindowsEnv {
		ss.Spec.Template.Spec.Containers[0].Image = constants.WindowsImageOnMcr
		ss.Spec.Template.Spec.Containers[0].Command = []string{"Powershell.exe"}
		ss.Spec.Template.Spec.Containers[0].Args = []string{"-Command", constants.WindowsExecCmd}
	}
	return ss
}

// IsDatastoreBelongsToDatacenterSpecifiedInConfig checks whether the given
// datastoreURL belongs to the datacenter specified in the vSphere.conf file.
func IsDatastoreBelongsToDatacenterSpecifiedInConfig(e2eTestConfig *config.E2eTestConfig,
	datastoreURL string) bool {
	var datacenters []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finder := find.NewFinder(e2eTestConfig.VcClient.Client, false)
	//cfg := vs.TestConfig
	//gomega.Expect(err).NotTo(gomega.HaveOccurred())

	dcList := strings.Split(e2eTestConfig.TestInput.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}
	for _, dc := range datacenters {
		defaultDatacenter, _ := finder.Datacenter(ctx, dc)
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err := GetDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		if defaultDatastore != nil && err == nil {
			return true
		}
	}

	// Loop through all datacenters specified in conf file, and cannot find this
	// given datastore.
	return false
}

func VerifyVolumeExistInSupervisorCluster(pvcName string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svcClient, svNamespace := GetSvcClientAndNamespace()
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

func WaitTillVolumeIsDeletedInSvc(pvcName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v to check whether volume  %s is deleted", timeout, pvcName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		svcClient, svNamespace := GetSvcClientAndNamespace()
		svPvc, err := svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(ctx,
			pvcName, metav1.GetOptions{})
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
func GetCnsNodeVMAttachmentByName(ctx context.Context,
	f *framework.Framework, expectedInstanceName string,
	crdVersion string, crdGroup string) *cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment {
	k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
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

// VerifyIsAttachedInSupervisor verifies the crd instance is attached in
// supervisor.
func VerifyIsAttachedInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdVersion string, crdGroup string) {
	instance := GetCnsNodeVMAttachmentByName(ctx, f, expectedInstanceName, crdVersion, crdGroup)
	if instance != nil {
		framework.Logf("instance attached found to be : %t\n", instance.Status.Attached)
		gomega.Expect(instance.Status.Attached).To(gomega.BeTrue())
	}
	gomega.Expect(instance).NotTo(gomega.BeNil())
}

// VerifyIsDetachedInSupervisor verifies the crd instance is detached from
// supervisor.
func VerifyIsDetachedInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdVersion string, crdGroup string) {
	instance := GetCnsNodeVMAttachmentByName(ctx, f, expectedInstanceName, crdVersion, crdGroup)
	if instance != nil {
		framework.Logf("instance attached found to be : %t\n", instance.Status.Attached)
		gomega.Expect(instance.Status.Attached).To(gomega.BeFalse())
	}
	gomega.Expect(instance).To(gomega.BeNil())
}

// verifyPodCreation helps to create/verify and delete the pod in given
// namespace. It takes client, namespace, pvc, pv as input.
func VerifyPodCreation(ctx context.Context, e2eTestConfig *config.E2eTestConfig,
	f *framework.Framework, client clientset.Interface, namespace string,
	pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) {
	defer ginkgo.GinkgoRecover()
	ginkgo.By("Create pod and wait for this to be in running phase")
	pod, err := CreatePod(ctx, e2eTestConfig, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// svcPVCName refers to PVC Name in the supervisor cluster.
	svcPVCName := pv.Spec.CSI.VolumeHandle

	ginkgo.By(fmt.Sprintf("Verify cnsnodevmattachment is created with name : %s ", pod.Spec.NodeName))
	VerifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
		constants.CrdCNSNodeVMAttachment, constants.CrdVersion, constants.CrdGroup, true)
	VerifyIsAttachedInSupervisor(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
		constants.CrdVersion, constants.CrdGroup)

	ginkgo.By("Deleting the pod")
	err = fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := WaitForVolumeDetachedFromNode(client, pv.Spec.CSI.VolumeHandle,
		pod.Spec.NodeName, e2eTestConfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

	ginkgo.By("Verify CnsNodeVmAttachment CRDs are deleted")
	VerifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
		constants.CrdCNSNodeVMAttachment, constants.CrdVersion, constants.CrdGroup, false)

}

// verifyCRDInSupervisor is a helper method to check if a given crd is
// created/deleted in the supervisor cluster. This method will fetch the list
// of CRD Objects for a given crdName, Version and Group and then verifies
// if the given expectedInstanceName exist in the list.
func VerifyCRDInSupervisorWithWait(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string,
	isCreated bool) {
	k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	var instanceFound bool

	const timeout time.Duration = 30
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(constants.Poll) {
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

// VerifyCRDInSupervisor is a helper method to check if a given crd is
// created/deleted in the supervisor cluster. This method will fetch the list
// of CRD Objects for a given crdName, Version and Group and then verifies
// if the given expectedInstanceName exist in the list.
func VerifyCRDInSupervisor(ctx context.Context,
	f *framework.Framework, expectedInstanceName string,
	crdName string, crdVersion string, crdGroup string, isCreated bool) {
	k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
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
				ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v", instance,
					expectedInstanceName))
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

// VerifyCNSFileAccessConfigCRDInSupervisor is a helper method to check if a
// given crd is created/deleted in the supervisor cluster. This method will
// fetch the list of CRD Objects for a given crdName, Version and Group and then
// verifies if the given expectedInstanceName exist in the list.
func VerifyCNSFileAccessConfigCRDInSupervisor(ctx context.Context,
	expectedInstanceName string, crdName string, crdVersion string,
	crdGroup string, isCreated bool) {
	// Adding an explicit wait time for the recounciler to refresh the status.
	time.Sleep(30 * time.Second)

	k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
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

// WaitTillCNSFileAccesscrdDeleted is a helper method to check if a
// given crd is created/deleted in the supervisor cluster. This method will
// fetch the list of CRD Objects for a given crdName, Version and Group and then
// verifies if the given expectedInstanceName exist in the list.
func WaitTillCNSFileAccesscrdDeleted(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string,
	isCreated bool) error {
	return wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			framework.Logf("Waiting for crd %s to disappear", expectedInstanceName)

			k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
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

// VerifyEntityReferenceForKubEntities takes context,client, pv, pvc and pod
// as inputs and verifies crdCNSVolumeMetadata is attached.
func VerifyEntityReferenceForKubEntities(ctx context.Context, f *framework.Framework,
	client clientset.Interface, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim,
	pod *v1.Pod) {
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", pv.Spec.CSI.VolumeHandle,
		pod.Spec.NodeName))
	volumeID := pv.Spec.CSI.VolumeHandle
	// svcPVCName refers to PVC Name in the supervisor cluster.
	svcPVCName := volumeID
	volumeID = GetVolumeIDFromSupervisorCluster(svcPVCName)
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

	VerifyEntityReferenceInCRDInSupervisor(ctx, f, pv.Spec.CSI.VolumeHandle, constants.CrdCNSVolumeMetadatas,
		constants.CrdVersion, constants.CrdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
	VerifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+podUID, constants.CrdCNSVolumeMetadatas,
		constants.CrdVersion, constants.CrdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
	VerifyEntityReferenceInCRDInSupervisor(ctx, f, gcClusterID+pvUID, constants.CrdCNSVolumeMetadatas,
		constants.CrdVersion, constants.CrdGroup, true, pv.Spec.CSI.VolumeHandle, false, nil, false)
}

// VerifyEntityReferenceInCRDInSupervisor is a helper method to check
// CnsVolumeMetadata CRDs exists and has expected labels.
func VerifyEntityReferenceInCRDInSupervisor(ctx context.Context, f *framework.Framework,
	expectedInstanceName string, crdName string, crdVersion string, crdGroup string,
	isCreated bool, volumeNames string, checkLabel bool, labels map[string]string,
	labelPresent bool) {
	k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
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

// TrimQuotes takes a quoted string as input and returns the same string unquoted.
func TrimQuotes(str string) string {
	str = strings.TrimPrefix(str, "\"")
	str = strings.TrimSuffix(str, " ")
	str = strings.TrimSuffix(str, "\"")

	return str
}

// ReadConfigFromSecretString takes input string of the form:
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
func ReadConfigFromSecretString(cfg string) (config.E2eTestConfig, error) {
	var config1 config.E2eTestConfig
	var testInput config.TestInputData
	var netPerm config.NetPermissionConfig
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
				testInput.Global.VCenterHostname = TrimQuotes(value)
				fmt.Printf("Key: VirtualCenter, Value: %s\n", value)
			}
			continue
		}
		key = words[0]
		value = TrimQuotes(words[1])
		var strconvErr error
		switch key {
		case "insecure-flag":
			if strings.Contains(value, "true") {
				testInput.Global.InsecureFlag = true
			} else {
				testInput.Global.InsecureFlag = false
			}
		case "cluster-id":
			testInput.Global.ClusterID = value
		case "cluster-distribution":
			testInput.Global.ClusterDistribution = value
		case "user":
			testInput.Global.User = value
		case "password":
			testInput.Global.Password = value
		case "datacenters":
			testInput.Global.Datacenters = value
		case "port":
			testInput.Global.VCenterPort = value
		case "cnsregistervolumes-cleanup-intervalinmin":
			testInput.Global.CnsRegisterVolumesCleanupIntervalInMin, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "topology-categories":
			testInput.Labels.TopologyCategories = value
		case "global-max-snapshots-per-block-volume":
			testInput.Snapshot.GlobalMaxSnapshotsPerBlockVolume, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "csi-fetch-preferred-datastores-intervalinmin":
			testInput.Global.CSIFetchPreferredDatastoresIntervalInMin, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "query-limit":
			testInput.Global.QueryLimit, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "list-volume-threshold":
			testInput.Global.ListVolumeThreshold, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "ca-file":
			testInput.Global.CaFile = value
		case "supervisor-id":
			testInput.Global.SupervisorID = value
		case "targetvSANFileShareClusters":
			testInput.Global.TargetVsanFileShareClusters = value
		case "fileVolumeActivated":
			testInput.Global.FileVolumeActivated, strconvErr = strconv.ParseBool(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		case "ips":
			netPerm.Ips = value
		case "permissions":
			netPerm.Permissions = permissions
		case "rootsquash":
			netPerm.RootSquash = rootSquash
		default:
			return config1, fmt.Errorf("unknown key %s in the input string", key)
		}
	}
	config1.TestInput = &testInput
	return config1, nil
}

// WriteConfigToSecretString takes in a structured config data and serializes
// that into a string.
func WriteConfigToSecretString(cfg config.E2eTestConfig) (string, error) {
	result := fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\ncluster-id = \"%s\"\ncluster-distribution = \"%s\"\n"+
		"csi-fetch-preferred-datastores-intervalinmin = %d\n"+"query-limit = \"%d\"\n"+
		"list-volume-threshold = \"%d\"\n\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"\n\n"+
		"[Snapshot]\nglobal-max-snapshots-per-block-volume = %d\n\n"+
		"[Labels]\ntopology-categories = \"%s\"",
		cfg.TestInput.Global.InsecureFlag, cfg.TestInput.Global.ClusterID, cfg.TestInput.Global.ClusterDistribution,
		cfg.TestInput.Global.CSIFetchPreferredDatastoresIntervalInMin, cfg.TestInput.Global.QueryLimit,
		cfg.TestInput.Global.ListVolumeThreshold,
		cfg.TestInput.Global.VCenterHostname, cfg.TestInput.Global.User, cfg.TestInput.Global.Password,
		cfg.TestInput.Global.Datacenters, cfg.TestInput.Global.VCenterPort,
		cfg.TestInput.Snapshot.GlobalMaxSnapshotsPerBlockVolume,
		cfg.TestInput.Labels.TopologyCategories)
	return result, nil
}

// Function to create CnsRegisterVolume spec, with given FCD ID and PVC name.
func GetCNSRegisterVolumeSpec(ctx context.Context, namespace string, fcdID string,
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
func CreateCNSRegisterVolume(ctx context.Context, restConfig *rest.Config,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume) error {

	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Create CNSRegisterVolume")
	err = cnsOperatorClient.Create(ctx, cnsRegisterVolume)

	return err
}

// Query CNS Register volume. Returns true if the CNSRegisterVolume is
// available otherwise false.
func QueryCNSRegisterVolume(ctx context.Context, restClientConfig *rest.Config,
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
func VerifyBidirectionalReferenceOfPVandPVC(ctx context.Context, client clientset.Interface,
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
func GetCNSRegistervolume(ctx context.Context, restClientConfig *rest.Config,
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
func UpdateCNSRegistervolume(ctx context.Context, restClientConfig *rest.Config,
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
					Image:   constants.BusyBoxImageOnGcr,
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

// WriteDataOnFileFromPod writes specified data from given Pod at the given.
func WriteDataOnFileFromPod(namespace string, e2eTestConfig *config.E2eTestConfig,
	podName string, filePath string, dataToWrite string) {
	var shellExec, cmdArg, syncCmd string
	if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
		shellExec = "Powershell.exe"
		cmdArg = "-Command"
	} else {
		shellExec = "/bin/sh"
		cmdArg = "-c"
		syncCmd = fmt.Sprintf("echo '%s' >> %s && sync", dataToWrite, filePath)
	}

	// Command to write data and sync it
	wrtiecmd := []string{"exec", podName, "--namespace=" + namespace, "--", shellExec, cmdArg, syncCmd}
	e2ekubectl.RunKubectlOrDie(namespace, wrtiecmd...)
}

// ReadFileFromPod read data from given Pod and the given file.
func ReadFileFromPod(namespace string,
	e2eTestConfig *config.E2eTestConfig, podName string, filePath string) string {
	var output string
	var shellExec, cmdArg string
	if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
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

// GetPersistentVolumeClaimSpecFromVolume gets vsphere persistent volume spec
// with given selector labels and binds it to given pv.
func GetPersistentVolumeClaimSpecFromVolume(namespace string, pvName string,
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

// GetPersistentVolumeSpecFromVolume gets static PV volume spec with given
// Volume ID, Reclaim Policy and labels.
func GetPersistentVolumeSpecFromVolume(volumeID string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy,
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
				Driver:       constants.E2evSphereCSIDriverName,
				VolumeHandle: volumeID,
				FSType:       "nfs4",
				ReadOnly:     true,
			},
		},
		Prebind: nil,
	}
	// Annotation needed to delete a statically created pv.
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = constants.E2evSphereCSIDriverName

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

// GetClusterComputeResource returns the clusterComputeResource and vSANClient.
func GetClusterComputeResource(ctx context.Context,
	vs *config.E2eTestConfig) ([]*object.ClusterComputeResource, *vsan.VsanClient) {
	var err error
	if clusterComputeResource == nil {
		clusterComputeResource, vsanHealthClient, err = vcutil.GetClusterName(ctx, vs)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return clusterComputeResource, vsanHealthClient
}

// VsanQueryObjectIdentities return list of vsan uuids
// example: For a PVC, It returns the vSAN object UUIDs to their identities
// It return vsanObjuuid like [4336525f-7813-d78a-e3a4-02005456da7e]
func QueryVsanObjectIdentities(ctx context.Context, c *vsan.VsanClient,
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

// VsanObjIndentities returns the vsanObjectsUUID.
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
	clusterComputeResource, vsanHealthClient = GetClusterComputeResource(ctx, vs)

	for _, cluster := range clusterComputeResource {
		if strings.Contains(cluster.Name(), computeCluster) {
			// Fix for NotAuthenticated issue
			//bootstrap()

			clusterConfig, err := QueryVsanObjectIdentities(ctx, vsanHealthClient, cluster.Reference())
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

// runCommandOnESX executes ssh commands on the give ESX host and returns the bash
// result.
func RunCommandOnESX(vs *config.E2eTestConfig, username string, addr string, cmd string) (string, error) {
	// Authentication.
	config := &ssh.ClientConfig{
		User:            username,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.KeyboardInteractive(HostLogin),
		},
	}

	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, addr)
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

// StopHostDOnHost executes hostd stop service commands on the given ESX host
func StopHostDOnHost(ctx context.Context, vs *config.E2eTestConfig, addr string) {
	framework.Logf("Stopping hostd service on the host  %s ...", addr)
	stopHostCmd := fmt.Sprintf("/etc/init.d/hostd %s", constants.StopOperation)
	_, err := RunCommandOnESX(vs, "root", addr, stopHostCmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = vcutil.WaitForHostConnectionState(ctx, vs, addr, "notResponding")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// StartHostDOnHost executes hostd start service commands on the given ESX host
func StartHostDOnHost(ctx context.Context, vs *config.E2eTestConfig, addr string) {
	framework.Logf("Starting hostd service on the host  %s ...", addr)
	startHostDCmd := fmt.Sprintf("/etc/init.d/hostd %s", constants.StartOperation)
	_, err := RunCommandOnESX(vs, "root", addr, startHostDCmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = vcutil.WaitForHostConnectionState(ctx, vs, addr, "connected")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	output := GetHostDStatusOnHost(vs, addr)
	gomega.Expect(strings.Contains(output, "hostd is running.")).NotTo(gomega.BeFalse())
}

// GetHostDStatusOnHost executes hostd status service commands on the given ESX host
func GetHostDStatusOnHost(vs *config.E2eTestConfig, addr string) string {
	framework.Logf("Running status check on hostd service for the host  %s ...", addr)
	statusHostDCmd := fmt.Sprintf("/etc/init.d/hostd %s", constants.StatusOperation)
	output, err := RunCommandOnESX(vs, "root", addr, statusHostDCmd)
	framework.Logf("hostd status command output is %q:", output)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return output
}

// GetPersistentVolumeSpecWithStorageclass is to create PV volume spec with
// given FCD ID, Reclaim Policy and labels.
func GetPersistentVolumeSpecWithStorageclass(volumeHandle string,
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
				Driver:       constants.E2evSphereCSIDriverName,
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
	annotations["pv.kubernetes.io/provisioned-by"] = constants.E2evSphereCSIDriverName
	pv.Annotations = annotations
	return pv
}

// GetPVCSpecWithPVandStorageClass is to create PVC spec with given PV, storage
// class and label details.
func GetPVCSpecWithPVandStorageClass(pvcName string, namespace string, labels map[string]string,
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

// WaitForEvent waits for and event with specified message substr for a given
// object name.
func WaitForEvent(ctx context.Context, client clientset.Interface,
	namespace string, substr string, name string) error {
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, 2*constants.PollTimeout, true,
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

// BringSvcK8sAPIServerDown function moves the static kube-apiserver.yaml out
// of k8's manifests directory. It takes VC IP and SV K8's master IP as input.
func BringSvcK8sAPIServerDown(ctx context.Context,
	vc string, vs *config.E2eTestConfig) error {
	file := "master.txt"
	token := "token.txt"

	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, vc)
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
		token, constants.KubeAPIPath, constants.KubeAPIfile)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err = fssh.SSH(ctx, sshCmd, vc, framework.TestContext.Provider)
	time.Sleep(constants.KubeAPIRecoveryTime)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// BringSvcK8sAPIServerUp function moves the static kube-apiserver.yaml to
// k8's manifests directory. It takes VC IP and SV K8's master IP as input.
func BringSvcK8sAPIServerUp(ctx context.Context,
	vs *config.E2eTestConfig, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, vc, healthStatus string) error {

	// Read hosts sshd port number
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, vc)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vc = ip + ":" + portNum

	sshCmd := fmt.Sprintf("sshpass -f token.txt ssh root@$(awk 'FNR == 6 {print $2}' master.txt) "+
		"-o 'StrictHostKeyChecking no' 'mv /root/%s %s'", constants.KubeAPIfile, constants.KubeAPIPath)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, vc)
	result, err := fssh.SSH(ctx, sshCmd, vc, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	ginkgo.By(fmt.Sprintf("polling for %v minutes...", constants.HealthStatusPollTimeout))
	err = PvcHealthAnnotationWatcher(ctx, client, pvclaim, healthStatus)
	if err != nil {
		return err
	}
	return nil
}

// PvcHealthAnnotationWatcher polls the health status of pvc and returns error
// if any.
func PvcHealthAnnotationWatcher(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, healthStatus string) error {
	framework.Logf("Waiting for health annotation for pvclaim %v", pvclaim.Name)
	waitErr := wait.PollUntilContextTimeout(ctx, constants.HealthStatusPollInterval,
		constants.HealthStatusPollTimeout, true,
		func(ctx context.Context) (bool, error) {
			framework.Logf("wait for next poll %v", constants.HealthStatusPollInterval)
			pvc, err := client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(ctx,
				pvclaim.Name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			if pvc.Annotations[constants.PvcHealthAnnotation] == healthStatus {
				framework.Logf("health annonation added :%v", pvc.Annotations[constants.PvcHealthAnnotation])
				return true, nil
			}
			return false, nil
		})
	return waitErr
}

// WaitForNamespaceToGetDeleted waits for a namespace to get deleted or until
// timeout occurs, whichever comes first.
func WaitForNamespaceToGetDeleted(ctx context.Context, c clientset.Interface,
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

// WaitForCNSRegisterVolumeToGetCreated waits for a cnsRegisterVolume to get
// created or until timeout occurs, whichever comes first.
func WaitForCNSRegisterVolumeToGetCreated(ctx context.Context, restConfig *rest.Config, namespace string,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for CnsRegisterVolume %v, namespace: %s,  to get created",
		timeout, cnsRegisterVolume, namespace)

	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		cnsRegisterVolume = GetCNSRegistervolume(ctx, restConfig, cnsRegisterVolume)
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
func WaitForCNSRegisterVolumeToGetDeleted(ctx context.Context, restConfig *rest.Config, namespace string,
	cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for cnsRegisterVolume %v to get deleted", timeout, cnsRegisterVolume)

	cnsRegisterVolumeName := cnsRegisterVolume.GetName()
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		flag := QueryCNSRegisterVolume(ctx, restConfig, cnsRegisterVolumeName, namespace)
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
func GetK8sMasterIPs(ctx context.Context, client clientset.Interface) []string {
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

// ToggleCSIMigrationFeatureGatesOnKubeControllerManager adds/removes
// CSIMigration and CSIMigrationvSphere feature gates to/from
// kube-controller-manager.
func ToggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx context.Context, vs *config.E2eTestConfig,
	client clientset.Interface, add bool) error {
	nimbusGeneratedK8sVmPwd := env.GetAndExpectStringEnvVar(constants.NimbusK8sVmPwd)
	v, err := client.Discovery().ServerVersion()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	v1, err := version.NewVersion(v.GitVersion)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	v2, err := version.NewVersion("v1.25.0")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if v1.LessThan(v2) {
		sshCmd := ""
		if !vs.TestInput.ClusterFlavor.VanillaCluster {
			return fmt.Errorf(
				"'toggleCSIMigrationFeatureGatesToKubeControllerManager' is implemented for vanilla cluster alone")
		}
		if add {
			sshCmd =
				"sed -i -e 's/CSIMigration=false,CSIMigrationvSphere=false/CSIMigration=true,CSIMigrationvSphere=true/g' " +
					constants.KcmManifest
		} else {
			sshCmd = "sed -i '/CSIMigration/d' " + constants.KcmManifest
		}
		grepCmd := "grep CSIMigration " + constants.KcmManifest
		k8sMasterIPs := GetK8sMasterIPs(ctx, client)
		for _, k8sMasterIP := range k8sMasterIPs {
			framework.Logf("Invoking command '%v' on host %v", grepCmd, k8sMasterIP)
			sshClientConfig := &ssh.ClientConfig{
				User: "root",
				Auth: []ssh.AuthMethod{
					ssh.Password(nimbusGeneratedK8sVmPwd),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			}
			result, err := SshExec(sshClientConfig, vs, k8sMasterIP, grepCmd)

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
						constants.KcmManifest
				} else {
					return nil
				}
			}
			framework.Logf("Invoking command %v on host %v", sshCmd, k8sMasterIP)
			result, err = SshExec(sshClientConfig, vs, k8sMasterIP, sshCmd)
			if err != nil || result.Code != 0 {
				fssh.LogResult(result)
				return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s", sshCmd, k8sMasterIP, err)
			}
			restartKubeletCmd := "systemctl restart kubelet"
			framework.Logf("Invoking command '%v' on host %v", restartKubeletCmd, k8sMasterIP)
			result, err = SshExec(sshClientConfig, vs, k8sMasterIP, restartKubeletCmd)
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
			"Waiting for 'kube-controller-manager' controller pod to come up within %v seconds", constants.PollTimeout*2)
		label := labels.SelectorFromSet(labels.Set(map[string]string{"component": "kube-controller-manager"}))
		_, err = fpod.WaitForPodsWithLabelRunningReady(ctx,
			client, constants.KubeSystemNamespace, label, len(k8sMasterIPs), constants.PollTimeout*2)
		if err == nil {
			framework.Logf("'kube-controller-manager' controller pod is up and ready within %v seconds", constants.PollTimeout*2)
		} else {
			framework.Logf(
				"'kube-controller-manager' controller pod is not up and/or ready within %v seconds", constants.PollTimeout*2)
		}
	}
	return err
}

// sshExec runs a command on the host via ssh.
func SshExec(sshClientConfig *ssh.ClientConfig,
	vs *config.E2eTestConfig, host string, cmd string) (fssh.Result, error) {
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

// createPod with given claims based on node selector.
func CreatePod(ctx context.Context, e2eTestConfig *config.E2eTestConfig,
	client clientset.Interface, namespace string, nodeSelector map[string]string,
	pvclaims []*v1.PersistentVolumeClaim, isPrivileged bool, command string) (*v1.Pod, error) {
	securityLevel := api.LevelBaseline
	if isPrivileged {
		securityLevel = api.LevelPrivileged
	}
	pod := fpod.MakePod(namespace, nodeSelector, pvclaims, securityLevel, command)
	if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
		var commands []string
		if (len(command) == 0) || (command == constants.ExecCommand) {
			commands = []string{"Powershell.exe", "-Command ", constants.WindowsExecCmd}
		} else if command == constants.ExecRWXCommandPod {
			commands = []string{"Powershell.exe", "-Command ", constants.WindowsExecRWXCommandPod}
		} else if command == constants.ExecRWXCommandPod1 {
			commands = []string{"Powershell.exe", "-Command ", constants.WindowsExecRWXCommandPod1}
		} else {
			commands = []string{"Powershell.exe", "-Command", command}
		}
		pod.Spec.Containers[0].Image = constants.WindowsImageOnMcr
		pod.Spec.Containers[0].Command = commands
		pod.Spec.Containers[0].VolumeMounts[0].MountPath = pod.Spec.Containers[0].VolumeMounts[0].MountPath + "/"
	} else {
		pod.Spec.Containers[0].Image = e2eTestConfig.TestInput.BusyBoxGcr.Image
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

func GetDeploymentSpec(ctx context.Context, client clientset.Interface, replicas int32,
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

// CreateDeployment create a deployment with 1 replica for given pvcs and node
// selector.
func CreateDeployment(ctx context.Context, testConfig *config.E2eTestConfig, client clientset.Interface, replicas int32,
	podLabels map[string]string, nodeSelector map[string]string, namespace string,
	pvclaims []*v1.PersistentVolumeClaim, command string, isPrivileged bool, image string) (*appsv1.Deployment, error) {
	if len(command) == 0 {
		if testConfig.TestInput.TestBedInfo.WindowsEnv {
			command = constants.WindowsExecCmd
		} else {
			command = "trap exit TERM; while true; do sleep 1; done"
		}
	}
	deploymentSpec := GetDeploymentSpec(ctx, client, replicas, podLabels, nodeSelector, namespace,
		pvclaims, command, isPrivileged, image)
	if testConfig.TestInput.TestBedInfo.WindowsEnv {
		var commands []string
		if (len(command) == 0) || (command == constants.ExecCommand) {
			commands = []string{constants.WindowsExecCmd}
		} else if command == constants.ExecRWXCommandPod {
			commands = []string{constants.WindowsExecRWXCommandPod}
		} else if command == constants.ExecRWXCommandPod1 {
			commands = []string{constants.WindowsExecRWXCommandPod1}
		} else {
			commands = []string{command}
		}
		deploymentSpec.Spec.Template.Spec.Containers[0].Image = constants.WindowsImageOnMcr
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

// CreatePodForFSGroup helps create pod with fsGroup.
func CreatePodForFSGroup(ctx context.Context, client clientset.Interface, namespace string,
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
	pod.Spec.Containers[0].Image = constants.BusyBoxImageOnGcr
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
func GetPersistentVolumeSpecForFileShare(fileshareID string,
	persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, labels map[string]string,
	accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	pv := GetPersistentVolumeSpec(fileshareID, persistentVolumeReclaimPolicy, labels, constants.Nfs4FSType)
	pv.Spec.AccessModes = []v1.PersistentVolumeAccessMode{accessMode}
	return pv
}

// getPersistentVolumeClaimSpecForFileShare return the PersistentVolumeClaim
// spec in the specified namespace.
func GetPersistentVolumeClaimSpecForFileShare(namespace string, labels map[string]string,
	pvName string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	pvc := GetPersistentVolumeClaimSpec(namespace, labels, pvName)
	pvc.Spec.AccessModes = []v1.PersistentVolumeAccessMode{accessMode}
	return pvc
}

// SetClusterDistribution sets the input cluster-distribution in
// vsphere-config-secret.
func SetClusterDistribution(ctx context.Context, client clientset.Interface, clusterDistribution string) {
	framework.Logf("Cluster distribution to set is = %s", clusterDistribution)

	// Get the current cluster-distribution value from secret.
	currentSecret, err := client.CoreV1().Secrets(constants.CsiSystemNamespace).Get(ctx,
		constants.ConfigSecret, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Secret Name is %s", currentSecret.Name)

	// Read and map the content of csi-vsphere.conf to a variable.
	originalConf := string(currentSecret.Data[constants.VSphereCSIConf])
	cfg, err := ReadConfigFromSecretString(originalConf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Current value of cluster-distribution is.
	framework.Logf("Cluster-distribution value before modifying is = %s", cfg.TestInput.Global.ClusterDistribution)

	// Check if the cluster-distribution value is as required or reset.
	if cfg.TestInput.Global.ClusterDistribution != clusterDistribution {
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

		modifiedConf := fmt.Sprintf(configContent, cfg.TestInput.Global.InsecureFlag,
			cfg.TestInput.Global.ClusterID, clusterDistribution, cfg.TestInput.Global.VCenterHostname,
			cfg.TestInput.Global.User, cfg.TestInput.Global.Password, cfg.TestInput.Global.Datacenters,
			cfg.TestInput.Global.VCenterPort, cfg.TestInput.Snapshot.GlobalMaxSnapshotsPerBlockVolume)

		// Set modified csi-vsphere.conf file and update.
		framework.Logf("Updating the secret")
		currentSecret.Data[constants.VSphereCSIConf] = []byte(modifiedConf)
		_, err := client.CoreV1().Secrets(constants.CsiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// TODO: Adding a explicit wait of two min for the Cluster-distribution to
		// reflect latest value. This should be replaced with a polling mechanism
		// to watch on csi-vsphere.conf inside the CSI containers.
		time.Sleep(time.Duration(2 * time.Minute))

		framework.Logf("Cluster distribution value is now set to = %s", clusterDistribution)

	} else {
		framework.Logf("Cluster-distribution value is already as expected, no changes done. Value is %s",
			cfg.TestInput.Global.ClusterDistribution)
	}
}

// ToggleCSIMigrationFeatureGatesOnK8snodes to toggle CSI migration feature
// gates on kublets for worker nodes.
func ToggleCSIMigrationFeatureGatesOnK8snodes(ctx context.Context,
	client clientset.Interface, vs *config.E2eTestConfig,
	shouldEnable bool, namespace string) {

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
			found = IsCSIMigrationFeatureGatesEnabledOnKubelet(ctx, client, node.Name)
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
			nodeIP := GetK8sNodeIP(&node)
			ToggleCSIMigrationFeatureGatesOnkublet(ctx, client, vs, nodeIP, shouldEnable)
			ginkgo.By("Wait for feature gates update on the k8s CSI node: " + node.Name)
			err = WaitForCSIMigrationFeatureGatesToggleOnkublet(ctx, client, node.Name, shouldEnable)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Uncordoning of node: " + node.Name)
			err = drain.RunCordonOrUncordon(&dh, &node, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		pods, err := fpod.GetPodsInNamespace(ctx, client, namespace, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(ctx, client, namespace, int(len(pods)),
			time.Duration(constants.PollTimeout*2))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// IsCSIMigrationFeatureGatesEnabledOnKubelet checks whether CSIMigration
// Feature Gates are enabled on CSI Node.
func IsCSIMigrationFeatureGatesEnabledOnKubelet(ctx context.Context, client clientset.Interface, nodeName string) bool {
	csinode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	found := false
	for annotation, value := range csinode.Annotations {
		framework.Logf("Annotation seen on CSI node - %s:%s", annotation, value)
		if annotation == constants.MigratedPluginAnnotation && strings.Contains(value, constants.VcpProvisionerName) {
			found = true
			break
		}
	}
	return found
}

// WaitForCSIMigrationFeatureGatesToggleOnkublet wait for CSIMigration Feature
// Gates toggle result on the csinode.
func WaitForCSIMigrationFeatureGatesToggleOnkublet(ctx context.Context,
	client clientset.Interface, nodeName string, added bool) error {
	var found bool
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll*5, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			csinode, err := client.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			found = false
			for annotation, value := range csinode.Annotations {
				framework.Logf("Annotation seen on CSI node - %s:%s", annotation, value)
				if annotation == constants.MigratedPluginAnnotation && strings.Contains(value, constants.VcpProvisionerName) {
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

// ToggleCSIMigrationFeatureGatesOnkublet adds/remove CSI migration feature
// gates to kubelet config yaml in given k8s node.
func ToggleCSIMigrationFeatureGatesOnkublet(ctx context.Context,
	client clientset.Interface, vs *config.E2eTestConfig, nodeIP string, shouldAdd bool) {
	grepCmd := "grep CSIMigration " + constants.KubeletConfigYaml
	framework.Logf("Invoking command '%v' on host %v", grepCmd, nodeIP)
	nimbusGeneratedK8sVmPwd := env.GetAndExpectStringEnvVar(constants.NimbusK8sVmPwd)
	sshClientConfig := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password(nimbusGeneratedK8sVmPwd),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	result, err := SshExec(sshClientConfig, vs, nodeIP, grepCmd)
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
  }" >>` + constants.KubeletConfigYaml
	} else if result.Code == 0 && !shouldAdd {
		sshCmd = fmt.Sprintf("head -n -5 %s > tmp.txt && mv tmp.txt %s",
			constants.KubeletConfigYaml, constants.KubeletConfigYaml)
	} else {
		return
	}
	framework.Logf("Invoking command '%v' on host %v", sshCmd, nodeIP)
	result, err = SshExec(sshClientConfig, vs, nodeIP, sshCmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", sshCmd, nodeIP))
	}
	restartKubeletCmd := "systemctl daemon-reload && systemctl restart kubelet"
	framework.Logf("Invoking command '%v' on host %v", restartKubeletCmd, nodeIP)
	result, err = SshExec(sshClientConfig, vs, nodeIP, restartKubeletCmd)
	if err != nil && result.Code != 0 {
		fssh.LogResult(result)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("command failed/couldn't execute command: %s on host: %v", restartKubeletCmd, nodeIP))
	}
}

// Returns host MOID for a given k8s node VM by referencing node VM ip
func GetHostMoref4K8sNode(ctx context.Context,
	vs *config.E2eTestConfig, client clientset.Interface, node *v1.Node) vim25types.ManagedObjectReference {
	vmIp2MoRefMap := vcutil.VmIpToMoRefMap(ctx, vs)
	return vmIp2MoRefMap[GetK8sNodeIP(node)]
}

// getK8sNodeIP returns the IP for the given k8s node.
func GetK8sNodeIP(node *v1.Node) string {
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

// ExpectedAnnotation polls for the given annotation in pvc and returns error
// if its not present.
func ExpectedAnnotation(ctx context.Context, client clientset.Interface,
	pvclaim *v1.PersistentVolumeClaim, annotation string) error {
	framework.Logf("Waiting for health annotation for pvclaim %v", pvclaim.Name)
	waitErr := wait.PollUntilContextTimeout(ctx, constants.HealthStatusPollInterval, constants.PollTimeout, true,
		func(ctx context.Context) (bool, error) {
			framework.Logf("wait for next poll %v", constants.HealthStatusPollInterval)
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
func GetRestConfigClient(vs *config.E2eTestConfig) *rest.Config {
	// Get restConfig.
	var err error
	if restConfig == nil {
		if vs.TestInput.ClusterFlavor.SupervisorCluster || vs.TestInput.ClusterFlavor.VanillaCluster {
			k8senv := env.GetAndExpectStringEnvVar("KUBECONFIG")
			restConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
		}
		if vs.TestInput.ClusterFlavor.GuestCluster {
			if k8senv := env.GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
				restConfig, err = clientcmd.BuildConfigFromFlags("", k8senv)
			}
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}
	return restConfig
}

// GetResizedStatefulSetFromManifest returns a StatefulSet from a manifest
// stored in fileName by adding namespace and a newSize.
func GetResizedStatefulSetFromManifest(ns string, e2eTestConfig *config.E2eTestConfig) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(constants.ManifestPath, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	ss, err = StatefulSetFromManifest(ssManifestFilePath, e2eTestConfig, ss)
	framework.ExpectNoError(err)
	return ss
}

// StatefulSetFromManifest returns a StatefulSet from a manifest stored in
// fileName in the Namespace indicated by ns.
func StatefulSetFromManifest(fileName string,
	e2eTestConfig *config.E2eTestConfig, ss *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	currentSize := ss.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage()
	newSize := currentSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	ss.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[v1.ResourceStorage] = newSize

	if e2eTestConfig.TestInput.TestBedInfo.WindowsEnv {
		ss.Spec.Template.Spec.Containers[0].Image = constants.WindowsImageOnMcr
		ss.Spec.Template.Spec.Containers[0].Command = []string{"Powershell.exe"}
		ss.Spec.Template.Spec.Containers[0].Args = []string{"-Command", constants.WindowsExecCmd}
	}
	return ss, nil
}

// CollectPodLogs collects logs from all the pods in the namespace
func CollectPodLogs(ctx context.Context, client clientset.Interface, namespace string) {
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
			err = WriteToFile("logs/"+pod.Name+cont.Name+filename, output)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
}

/*
This method works for 2 level topology enabled testbeds.
VerifyPVnodeAffinityAndPODnodedetailsForStatefulsets verifies that PV node Affinity rules
should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on
which PV is provisioned.
*/
func VerifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx context.Context,
	vs *config.E2eTestConfig,
	client clientset.Interface, statefulset *appsv1.StatefulSet,
	namespace string, zoneValues []string, regionValues []string) {
	ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := GetPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				// verify pv node affinity details
				pvRegion, pvZone, err = VerifyVolumeTopology(pv, zoneValues, regionValues)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
				framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
				if !(len(nodeList.Items) > 0) {
					framework.Failf("Unable to find ready and schedulable Node")
				}
				// verify node topology details
				err = VerifyPodLocation(&sspod, nodeList, pvZone, pvRegion)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Verify the attached volume match the one in CNS cache
				error := vcutil.VerifyVolumeMetadataInCNS(pv.Spec.CSI.VolumeHandle, vs,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
				gomega.Expect(error).NotTo(gomega.HaveOccurred())
			}
		}
	}
}

// IsCsiFssEnabled checks if the given CSI FSS is enabled or not, errors out if not found
func IsCsiFssEnabled(ctx context.Context, client clientset.Interface, namespace string, fss string) bool {
	fssCM, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, constants.CsiFssCM, metav1.GetOptions{})
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
		framework.Logf("FSS %s not found in the %s configmap in namespace %s", fss, constants.CsiFssCM, namespace)
	}
	return false
}

/*
This wrapper method is used to create the topology map of allowed topologies specified on VC.
TOPOLOGY_MAP = "region:region1;zone:zone1;building:building1;level:level1;rack:rack1,rack2,rack3"
*/
func CreateTopologyMapLevel5(topologyMapStr string) (map[string][]string, []string) {
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
func CreateAllowedTopolgies(topologyMapStr string) []v1.TopologySelectorLabelRequirement {
	topologyFeature := os.Getenv(constants.TopologyFeature)
	topologyMap, _ := CreateTopologyMapLevel5(topologyMapStr)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{}
	topoKey := ""
	if topologyFeature == constants.TopologyTkgHaName || topologyFeature == constants.PodVMOnStretchedSupervisor ||
		topologyFeature == constants.TopologyDomainIsolation {
		topoKey = constants.TkgHATopologyKey
	} else {
		topoKey = constants.Topologykey
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
func CreateAllowedTopologiesMap(allowedTopologies []v1.TopologySelectorLabelRequirement) map[string][]string {
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
	var StatefulSetPods = new(v1.PodList)
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
VerifyVolumeTopologyForLevel5 verifies that the pv node affinity details should match the
allowed topologies specified in the storage class.
This method returns true if allowed topologies of SC matches with the PV node
affinity details else return error and false.
*/
func VerifyVolumeTopologyForLevel5(pv *v1.PersistentVolume, allowedTopologiesMap map[string][]string) (bool, error) {
	if pv.Spec.NodeAffinity == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return false, fmt.Errorf("node Affinity rules for PV should exist in topology aware provisioning")
	}
	topologyFeature := os.Getenv(constants.TopologyFeature)
	for _, nodeSelector := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, topology := range nodeSelector.MatchExpressions {
			if val, ok := allowedTopologiesMap[topology.Key]; ok {
				framework.Logf("pv.Spec.NodeAffinity: %v, nodeSelector: %v", pv.Spec.NodeAffinity, nodeSelector)
				if !CompareStringLists(val, topology.Values) {
					if topologyFeature == constants.TopologyTkgHaName ||
						topologyFeature == constants.PodVMOnStretchedSupervisor ||
						topologyFeature == constants.TopologyDomainIsolation {
						return false, fmt.Errorf("pv node affinity details: %v does not match"+
							"with: %v in the allowed topologies", topology.Values, val)
					} else {
						return false, fmt.Errorf("PV node affinity details does not exist in the allowed " +
							"topologies specified in SC")
					}
				}
			} else {
				if topologyFeature == constants.TopologyTkgHaName ||
					topologyFeature == constants.PodVMOnStretchedSupervisor ||
					topologyFeature == constants.TopologyDomainIsolation {
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
func CompareStringLists(strList1 []string, strList2 []string) bool {
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
VerifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5 for Statefulset verifies that PV
node Affinity rules should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on
which PV is provisioned.
*/
func VerifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx context.Context,
	vs *config.E2eTestConfig,
	client clientset.Interface, statefulset *appsv1.StatefulSet, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	parallelStatefulSetCreation bool) error {
	allowedTopologiesMap := CreateAllowedTopologiesMap(allowedTopologies)
	var ssPodsBeforeScaleDown *v1.PodList
	var err error

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
				pv := GetPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

				// verify pv node affinity details as specified on SC
				ginkgo.By("Verifying PV node affinity details")
				res, err := VerifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
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
				res, err = VerifyPodLocationLevel5(&sspod, nodeList, allowedTopologiesMap)
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
				if !vs.TestInput.TestBedInfo.Multivc {
					err := vcutil.VerifyVolumeMetadataInCNS(pv.Spec.CSI.VolumeHandle, vs,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("multi vc case is not covered here yet")
					/*err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					if err != nil {
						return fmt.Errorf("error verifying volume metadata in CNS for multi-VC: %v", err)
					}*/
				}

			}
		}
	}

	return nil
}

/*
VerifyPodLocationLevel5 verifies that a pod is scheduled on a node that belongs to the
topology on which PV is provisioned.
This method returns true if all topology labels matches else returns false and error.
*/
func VerifyPodLocationLevel5(pod *v1.Pod, nodeList *v1.NodeList,
	allowedTopologiesMap map[string][]string) (bool, error) {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			for labelKey, labelValue := range node.Labels {
				if topologyValue, ok := allowedTopologiesMap[labelKey]; ok {
					if !Contains(topologyValue, labelValue) {
						return false, fmt.Errorf("pod: %s is not running on node located in %s", pod.Name, labelValue)
					}
				}
			}
		}
	}
	return true, nil
}

// check whether the slice Contains an element
func Contains(volumes []string, volumeID string) bool {
	for _, volumeUUID := range volumes {
		if volumeUUID == volumeID {
			return true
		}
	}
	return false
}

// udpate updates a statefulset, and it is only used within rest.go
func UpdateSts(c clientset.Interface, ns, name string, update func(ss *appsv1.StatefulSet)) *appsv1.StatefulSet {
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
func ScaleStatefulSetPods(c clientset.Interface, ss *appsv1.StatefulSet, count int32) (*appsv1.StatefulSet, error) {
	name := ss.Name
	ns := ss.Namespace
	StatefulSetPoll := 10 * time.Second
	StatefulSetTimeout := 10 * time.Minute
	framework.Logf("Scaling statefulset %s to %d", name, count)
	ss = UpdateSts(c, ns, name, func(ss *appsv1.StatefulSet) { *(ss.Spec.Replicas) = count })

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
ScaleDownStatefulSetPod util is used to perform scale down operation on StatefulSert Pods
and later verifies that after scale down operation volume gets detached from the node
and returns nil if no error found
*/
func ScaleDownStatefulSetPod(ctx context.Context, vs *config.E2eTestConfig, client clientset.Interface,
	statefulset *appsv1.StatefulSet, namespace string, replicas int32, parallelStatefulSetCreation bool) error {
	ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
	var ssPodsAfterScaleDown *v1.PodList
	var err error

	if parallelStatefulSetCreation {
		_, scaledownErr := ScaleStatefulSetPods(client, statefulset, replicas)
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
						pv := GetPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						if !vs.TestInput.TestBedInfo.Multivc {
							isDiskDetached, detachErr := vcutil.WaitForVolumeDetachedFromNode(
								vs, client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							if detachErr != nil {
								return detachErr
							}
							if !isDiskDetached {
								return fmt.Errorf("volume %q is not detached from the node %q",
									pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
							}
						} else {
							return fmt.Errorf("multi vc support is not there right now")
							/*
								isDiskDetached, detachErr := multiVCe2eVSphere.waitForVolumeDetachedFromNodeInMultiVC(
									client, pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
								if detachErr != nil {
									return detachErr
								}
								if !isDiskDetached {
									return fmt.Errorf("volume %q is not detached from the node %q",
										pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
								}*/
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
				if !vs.TestInput.TestBedInfo.Multivc {
					pv := GetPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					err := vcutil.VerifyVolumeMetadataInCNS(pv.Spec.CSI.VolumeHandle, vs,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("multi vc case is not covered here yet")
					/*pv := GetPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
					if err != nil {
						return fmt.Errorf("error verifying volume metadata in CNS for multi-VC: %v", err)
					}*/
				}
			}
		}
	}
	return nil
}

/*
ScaleUpStatefulSetPod util is used to perform scale up operation on StatefulSert Pods
and later verifies that after scale up operation volume get successfully attached to the node
and returns nil if no error found
*/
func ScaleUpStatefulSetPod(ctx context.Context, client clientset.Interface, vs *config.E2eTestConfig,
	statefulset *appsv1.StatefulSet, namespace string, replicas int32,
	parallelStatefulSetCreation bool) error {
	ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
	var ssPodsAfterScaleUp *v1.PodList
	var err error

	if parallelStatefulSetCreation {
		_, scaleupErr := ScaleStatefulSetPods(client, statefulset, replicas)
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
		err = fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace, constants.PollTimeout)
		if err != nil {
			return err
		}
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		for _, volumespec := range pod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				pv := GetPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
				var vmUUID string
				var exists bool
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if vs.TestInput.ClusterFlavor.VanillaCluster {
					vmUUID = GetNodeUUID(ctx, client, sspod.Spec.NodeName)
				} else if vs.TestInput.ClusterFlavor.SupervisorCluster {
					annotations := pod.Annotations
					vmUUID, exists = annotations[constants.VmUUIDLabel]
					if !exists {
						return fmt.Errorf("pod doesn't have %s annotation", constants.VmUUIDLabel)
					}
					if !vs.TestInput.TestBedInfo.Multivc {
						_, err := vcutil.GetVMByUUID(ctx, vs, vmUUID)
						if err != nil {
							return err
						}
					} else {
						return fmt.Errorf("multi vc case is not covered here yet")
						/*_, err := multiVCe2eVSphere.getVMByUUIDForMultiVC(ctx, vmUUID)
						if err != nil {
							return err
						}*/
					}
				}
				if !vs.TestInput.ClusterFlavor.GuestCluster {
					if !vs.TestInput.TestBedInfo.Multivc {
						if !vs.TestInput.TestBedInfo.RwxAccessMode {
							isDiskAttached, err := vcutil.IsVolumeAttachedToVM(client, vs, pv.Spec.CSI.VolumeHandle, vmUUID)
							if err != nil {
								return err
							}
							if !isDiskAttached {
								return fmt.Errorf("disk is not attached to the node")
							}
						}
						err = vcutil.VerifyVolumeMetadataInCNS(pv.Spec.CSI.VolumeHandle, vs,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						if err != nil {
							return err
						}
					} else {
						return fmt.Errorf("multi vc case is not covered here yet")
						/*if !constants.RwxAccessMode {
							isDiskAttached, err := vs.VerifyVolumeIsAttachedToVMInMultiVC(pv.Spec.CSI.VolumeHandle, vmUUID)
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
						}*/
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
func GetTopologySelector(topologyAffinityDetails map[string][]string,
	topologyCategories []string, level int,
	position ...int) []v1.TopologySelectorLabelRequirement {
	topologyFeature := os.Getenv(constants.TopologyFeature)
	var key string
	if topologyFeature == constants.TopologyTkgHaName || topologyFeature == constants.PodVMOnStretchedSupervisor {
		key = constants.TkgHATopologyKey
	} else {
		key = constants.Topologykey
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
			if topologyFeature == constants.TopologyTkgHaName || topologyFeature == constants.PodVMOnStretchedSupervisor {
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
VerifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5 for Deployment verifies that PV node
Affinity rules should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on which
PV is provisioned.
*/
func VerifyPVnodeAffinityAndPODnodedetailsForDeploymentSetsLevel5(ctx context.Context, vs *config.E2eTestConfig,
	client clientset.Interface, deployment *appsv1.Deployment, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	parallelDeplCreation bool) error {
	allowedTopologiesMap := CreateAllowedTopologiesMap(allowedTopologies)
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
				pv := GetPvFromClaim(client, deployment.Namespace, volumespec.PersistentVolumeClaim.ClaimName)

				// verify pv node affinity details as specified on SC
				ginkgo.By("Verifying PV node affinity details")
				res, err := VerifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
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
				res, err = VerifyPodLocationLevel5(&sspod, nodeList, allowedTopologiesMap)
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
				if !vs.TestInput.TestBedInfo.Multivc {
					err := vcutil.VerifyVolumeMetadataInCNS(pv.Spec.CSI.VolumeHandle, vs,
						volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("multi vc case is not covered here yet")
					/*
						err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						if err != nil {
							return err
						}
					*/
				}
			}
		}
	}

	return nil
}

/*
For Standalone Pod
VerifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5
verifyPVnodeAffinityAndPODnodedetailsFoStandalonePodLevel5 for Standalone Pod verifies that PV
node Affinity rules should match the topology constraints specified in the storage class.
Also it verifies that a pod is scheduled on a node that belongs to the topology on which PV
is provisioned.
*/
func VerifyPVnodeAffinityAndPODnodedetailsForStandalonePodLevel5(ctx context.Context,
	vs *config.E2eTestConfig,
	client clientset.Interface, pod *v1.Pod,
	allowedTopologies []v1.TopologySelectorLabelRequirement) error {
	allowedTopologiesMap := CreateAllowedTopologiesMap(allowedTopologies)
	for _, volumespec := range pod.Spec.Volumes {
		if volumespec.PersistentVolumeClaim != nil {
			// get pv details
			pv := GetPvFromClaim(client, pod.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
			if pv == nil {
				return fmt.Errorf("failed to get PV for claim: %s", volumespec.PersistentVolumeClaim.ClaimName)
			}

			// verify pv node affinity details as specified on SC
			ginkgo.By("Verifying PV node affinity details")
			res, err := VerifyVolumeTopologyForLevel5(pv, allowedTopologiesMap)
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
			res, err = VerifyPodLocationLevel5(pod, nodeList, allowedTopologiesMap)
			if err != nil {
				return fmt.Errorf("error verifying pod location: %v", err)
			}
			if !res {
				return fmt.Errorf("pod %v is not running on appropriate node as specified in allowed "+
					"topologies of Storage Class", pod.Name)
			}

			// Verify the attached volume matches the one in CNS cache
			if !vs.TestInput.TestBedInfo.Multivc {
				err := vcutil.VerifyVolumeMetadataInCNS(pv.Spec.CSI.VolumeHandle, vs,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
				if err != nil {
					return fmt.Errorf("error verifying volume metadata in CNS: %v", err)
				}
			} else {
				return fmt.Errorf("multi vc case is not covered here yet")
				/*err := verifyVolumeMetadataInCNSForMultiVC(&multiVCe2eVSphere, pv.Spec.CSI.VolumeHandle,
					volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, pod.Name)
				if err != nil {
					return fmt.Errorf("error verifying volume metadata in CNS for multi-VC: %v", err)
				}*/
			}
		}
	}
	return nil
}

func GetPersistentVolumeSpecWithStorageClassFCDNodeSelector(volumeHandle string,
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
				Driver:       constants.E2evSphereCSIDriverName,
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
	annotations["pv.kubernetes.io/provisioned-by"] = constants.E2evSphereCSIDriverName
	pv.Annotations = annotations
	pv.Spec.NodeAffinity = new(v1.VolumeNodeAffinity)
	pv.Spec.NodeAffinity.Required = new(v1.NodeSelector)
	pv.Spec.NodeAffinity.Required.NodeSelectorTerms = GetNodeSelectorTerms(allowedTopologies)
	return pv
}

func GetNodeSelectorTerms(allowedTopologies []v1.TopologySelectorLabelRequirement) []v1.NodeSelectorTerm {
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

// CreateKubernetesClientFromConfig creaates a newk8s client from given
// kubeConfig file.
func CreateKubernetesClientFromConfig(kubeConfigPath string) (clientset.Interface, error) {

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
func GetK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx context.Context,
	vs *config.E2eTestConfig,
	client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	containerName string) (string, string, error) {
	ignoreLabels := make(map[string]string)
	csiControllerPodName, grepCmdForFindingCurrentLeader := "", ""
	csiPods, err := fpod.GetPodsInNamespace(ctx, client, constants.CsiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var k8sMasterIP, kubeConfigPath string
	if vs.TestInput.ClusterFlavor.GuestCluster {
		k8sMasterIP = env.GetAndExpectStringEnvVar(constants.SvcMasterIP)
		kubeConfigPath = env.GetAndExpectStringEnvVar(constants.GcKubeConfigPath)
	} else {
		k8sMasterIPs := GetK8sMasterIPs(ctx, client)
		k8sMasterIP = k8sMasterIPs[0]
	}

	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, constants.VSphereCSIControllerPodNamePrefix) {
			// Putting the grepped logs for leader of container of different CSI pods
			// to same temporary file
			// NOTE: This is not valid for vsphere-csi-controller container as for
			// vsphere-csi-controller all the replicas will behave as leaders
			grepCmdForFindingCurrentLeader = "echo `kubectl logs " + csiPod.Name + " -n " +
				constants.CsiSystemNamespace + " " + containerName + " | grep 'successfully acquired lease' | " +
				"tail -1` 'podName:" + csiPod.Name + "' | tee -a leader.log"
			if vs.TestInput.ClusterFlavor.GuestCluster {
				grepCmdForFindingCurrentLeader = fmt.Sprintf("echo `kubectl logs "+csiPod.Name+" -n "+
					constants.CsiSystemNamespace+" "+containerName+" --kubeconfig %s "+" | grep 'successfully acquired lease' | "+
					"tail -1` 'podName:"+csiPod.Name+"' | tee -a leader.log", kubeConfigPath)
			}

			framework.Logf("Invoking command '%v' on host %v", grepCmdForFindingCurrentLeader,
				k8sMasterIP)
			result, err := SshExec(sshClientConfig, vs, k8sMasterIP,
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
	result, err := SshExec(sshClientConfig, vs, k8sMasterIP,
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
	result, err = SshExec(sshClientConfig, vs, k8sMasterIP,
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
	podData, err := client.CoreV1().Pods(constants.CsiSystemNamespace).Get(ctx, csiControllerPodName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	masterNodeName := podData.Spec.NodeName
	framework.Logf("Master node name %s where %s leader is running", masterNodeName, containerName)
	// Fetching IP address of master node where container leader is running
	k8sMasterNodeIP, err := GetMasterIpFromMasterNodeName(ctx, client, masterNodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Master node ip %s where %s leader is running", k8sMasterNodeIP, containerName)
	return csiControllerPodName, k8sMasterNodeIP, nil
}

// ExecDockerPauseNKillOnContainer pauses and then kills the particular CSI container on given master node
func ExecDockerPauseNKillOnContainer(sshClientConfig *ssh.ClientConfig,
	vs *config.E2eTestConfig, k8sMasterNodeIP string,
	containerName string, k8sVersion string) error {
	containerPauseCmd := ""
	containerKillCmd := ""
	k8sVer, err := strconv.ParseFloat(k8sVersion, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	containerID, err := WaitAndGetContainerID(sshClientConfig, vs, k8sMasterNodeIP, containerName, k8sVer)
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
	cmdResult, err := SshExec(sshClientConfig, vs, k8sMasterNodeIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, k8sMasterNodeIP,
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
func GetMasterIpFromMasterNodeName(ctx context.Context, client clientset.Interface,
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

func CreateParallelStatefulSets(client clientset.Interface, namespace string,
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

func CreateParallelStatefulSetSpec(e2eTestConfig *config.TestInputData,
	namespace string, no_of_sts int, replicas int32) []*appsv1.StatefulSet {
	stss := []*appsv1.StatefulSet{}
	var statefulset *appsv1.StatefulSet

	for i := 0; i < no_of_sts; i++ {
		scName := constants.DefaultNginxStorageClassName
		statefulset = GetStatefulSetFromManifest(e2eTestConfig, namespace)
		statefulset.Name = "thread-" + strconv.Itoa(i) + "-" + statefulset.Name
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &scName
		statefulset.Spec.Replicas = &replicas
		stss = append(stss, statefulset)
	}
	return stss
}

func CreateMultiplePVCsInParallel(ctx context.Context, client clientset.Interface, namespace string,
	storageclass *storagev1.StorageClass, count int, pvclaimlabels map[string]string) []*v1.PersistentVolumeClaim {
	var pvclaims []*v1.PersistentVolumeClaim
	for i := 0; i < count; i++ {
		pvclaim, err := CreatePVC(ctx, client, namespace, nil, "", storageclass, "")
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
func DeleteCsiControllerPodWhereLeaderIsRunning(ctx context.Context,
	client clientset.Interface, csi_controller_pod string) error {
	ignoreLabels := make(map[string]string)
	csiPods, err := fpod.GetPodsInNamespace(ctx, client, constants.CsiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	num_csi_pods := len(csiPods)
	// Collecting and dumping csi pod logs before deleting them
	CollectPodLogs(ctx, client, constants.CsiSystemNamespace)
	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, constants.VSphereCSIControllerPodNamePrefix) && csiPod.Name == csi_controller_pod {
			framework.Logf("Deleting the pod: %s", csiPod.Name)
			err = fpod.DeletePodWithWait(ctx, client, csiPod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}
	// wait for csi Pods to be in running ready state
	err = fpod.WaitForPodsRunningReady(ctx, client, constants.CsiSystemNamespace, int(num_csi_pods),
		time.Duration(constants.PollTimeout))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return nil
}

// get topology cluster lists
func ListTopologyClusterNames(topologyCluster string) []string {
	topologyClusterList := strings.Split(topologyCluster, ",")
	return topologyClusterList
}

// getHosts returns list of hosts and it takes clusterComputeResource as input.
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

// WaitForPvcToBeDeleted waits by polling for a particular pvc to be deleted in a namespace
func WaitForPvcToBeDeleted(ctx context.Context, client clientset.Interface, pvcName string, namespace string) error {
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeout, true,
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
func WaitForEventWithReason(client clientset.Interface, namespace string,
	name string, expectedErrMsg string) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	isFailureFound := false
	ginkgo.By("Checking for error in events related to pvc " + name)
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeoutShort, true,
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

// StopCSIPods function stops all the running csi pods
func StopCSIPods(ctx context.Context, client clientset.Interface, namespace string) (bool, error) {
	CollectPodLogs(ctx, client, constants.CsiSystemNamespace)
	isServiceStopped := false
	err := UpdateDeploymentReplicawithWait(client, 0, constants.VSphereCSIControllerPodNamePrefix,
		namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = true
	return isServiceStopped, err
}

// StartCSIPods function starts the csi pods and waits till all the pods comes up
func StartCSIPods(ctx context.Context, client clientset.Interface, csiReplicas int32,
	namespace string) (bool, error) {
	ignoreLabels := make(map[string]string)
	err := UpdateDeploymentReplicawithWait(client, csiReplicas, constants.VSphereCSIControllerPodNamePrefix,
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
		time.Duration(constants.PollTimeout))
	isServiceStopped := false
	return isServiceStopped, err
}

// waitForStsPodsToBeInRunningState function waits till all the pods comes up
func WaitForStsPodsToBeInReadyRunningState(ctx context.Context, client clientset.Interface, namespace string,
	statefulSets []*appsv1.StatefulSet) error {
	waitErr := wait.PollUntilContextTimeout(ctx, constants.PollTimeoutShort, constants.PollTimeoutShort*20, true,
		func(ctx context.Context) (bool, error) {
			for i := 0; i < len(statefulSets); i++ {
				fss.WaitForStatusReadyReplicas(ctx, client, statefulSets[i], *statefulSets[i].Spec.Replicas)
				pods := GetListOfPodsInSts(client, statefulSets[i])
				err := CheckMountForStsPods(client, statefulSets[i], constants.MountPath)
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
func EnableFullSyncTriggerFss(ctx context.Context, client clientset.Interface, namespace string, fss string) {
	fssCM, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, constants.CsiFssCM, metav1.GetOptions{})
	framework.Logf("%s configmap in namespace %s is %s", constants.CsiFssCM, namespace, fssCM)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	fssFound := false
	for k, v := range fssCM.Data {
		if fss == k && v != "true" {
			framework.Logf("FSS %s found in the %s configmap in namespace %s", fss, constants.CsiFssCM, namespace)
			fssFound = true
			fssCM.Data[fss] = "true"
			// Enable full sync fss by updating full sync field to true
			_, err = client.CoreV1().ConfigMaps(namespace).Update(ctx, fssCM, metav1.UpdateOptions{})
			framework.Logf("%s configmap in namespace %s is %s", constants.CsiFssCM, namespace, fssCM)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Collecting and dumping csi pod logs before killing them
			CollectPodLogs(ctx, client, constants.CsiSystemNamespace)
			csipods, err := client.CoreV1().Pods(constants.CsiSystemNamespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, pod := range csipods.Items {
				fpod.DeletePodOrFail(ctx, client, constants.CsiSystemNamespace, pod.Name)
			}
			err = fpod.WaitForPodsRunningReady(ctx, client, constants.CsiSystemNamespace, int(csipods.Size()),
				time.Duration(constants.PollTimeout))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			break
		} else if fss == k && v == "true" {
			framework.Logf("FSS %s found and is enabled in the %s configmap", fss, constants.CsiFssCM)
			fssFound = true
			break
		}
	}
	gomega.Expect(fssFound).To(gomega.BeTrue(),
		"FSS %s not found in the %s configmap in namespace %s", fss, constants.CsiFssCM, namespace)
}
func GetTriggerFullSyncCrd(ctx context.Context,
	cnsOperatorClient client.Client) *triggercsifullsyncv1alpha1.TriggerCsiFullSync {
	fullSyncCrd := &triggercsifullsyncv1alpha1.TriggerCsiFullSync{}
	err := cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: constants.CrdtriggercsifullsyncsName}, fullSyncCrd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(fullSyncCrd).NotTo(gomega.BeNil(),
		"couldn't find full sync crd: %s", constants.CrdtriggercsifullsyncsName)
	return fullSyncCrd
}

// waitForFullSyncToFinish waits for a given full sync to finish by checking
// InProgress field in trigger full sync crd
func WaitForFullSyncToFinish(ctx context.Context,
	cnsOperatorClient client.Client) error {
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeoutShort, true,
		func(ctx context.Context) (bool, error) {
			crd := GetTriggerFullSyncCrd(ctx, cnsOperatorClient)
			framework.Logf("crd is: %v", crd)
			if !crd.Status.InProgress {
				return true, nil
			}
			if crd.Status.Error != "" {
				return false, fmt.Errorf("full sync failed with error: %s", crd.Status.Error)
			}
			return false, nil
		})
	return waitErr
}

// TriggerFullSync triggers 2 full syncs on demand
func TriggerFullSync(ctx context.Context, vs *config.E2eTestConfig, cnsOperatorClient client.Client) {
	err := WaitForFullSyncToFinish(ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
	crd := GetTriggerFullSyncCrd(ctx, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)
	UpdateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd)
	err = WaitForFullSyncToFinish(ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
	crd_updated := GetTriggerFullSyncCrd(ctx, cnsOperatorClient)
	framework.Logf("INFO: full sync crd details: %v", crd)

	UpdateTriggerFullSyncCrd(ctx, cnsOperatorClient, *crd_updated)
	err = WaitForFullSyncToFinish(ctx, cnsOperatorClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Full sync did not finish in given time")
}

// updateTriggerFullSyncCrd triggers full sync by updating TriggerSyncID
// value to  LastTriggerSyncID +1 in full sync crd
func UpdateTriggerFullSyncCrd(ctx context.Context, cnsOperatorClient client.Client,
	crd triggercsifullsyncv1alpha1.TriggerCsiFullSync) {
	framework.Logf("instance is %v before update", crd)
	lastSyncId := crd.Status.LastTriggerSyncID
	triggerSyncID := lastSyncId + 1
	crd.Spec.TriggerSyncID = triggerSyncID

	err := cnsOperatorClient.Update(ctx, &crd)
	framework.Logf("Error is %v", err)

	if apierrors.IsConflict(err) {
		latest_crd := GetTriggerFullSyncCrd(ctx, cnsOperatorClient)
		framework.Logf("INFO: full sync crd details: %v", latest_crd)
		lastSyncId := latest_crd.Status.LastTriggerSyncID
		triggerSyncID := lastSyncId + 1
		latest_crd.Spec.TriggerSyncID = triggerSyncID
		err = cnsOperatorClient.Update(ctx, latest_crd)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("instance is %v after update", crd)
}

// waitAndGetContainerID waits and fetches containerID of a given containerName
func WaitAndGetContainerID(sshClientConfig *ssh.ClientConfig, vs *config.E2eTestConfig, k8sMasterIP string,
	containerName string, k8sVersion float64) (string, error) {
	containerId := ""
	cmdToGetContainerId := ""
	waitErr := wait.PollUntilContextTimeout(context.Background(), constants.Poll*5, constants.PollTimeout*4, true,
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
			dockerContainerInfo, err := SshExec(sshClientConfig, vs, k8sMasterIP,
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

// assignPolicyToWcpNamespace assigns a set of storage policies to a wcp namespace
func AssignPolicyToWcpNamespace(client clientset.Interface, vs *config.E2eTestConfig, ctx context.Context,
	namespace string, policyNames []string, resourceQuotaLimit string) {
	var err error
	sessionId := CreateVcSession4RestApis(ctx, vs)
	curlStr := ""
	policyNamesArrLength := len(policyNames)
	defRqLimit := strings.Split(resourceQuotaLimit, "Gi")[0]
	limit, err := strconv.Atoi(defRqLimit)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	limit *= 953 // to convert gb to mebibytes

	// Read hosts sshd port number
	vcIp, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, vs.TestInput.TestBedInfo.VcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vcAddress := vcIp + ":" + portNum

	if policyNamesArrLength >= 1 {
		curlStr += fmt.Sprintf(`{ "limit": %d, "policy": "%s"}`, limit, vcutil.GetSpbmPolicyID(policyNames[0], vs))
	}
	if policyNamesArrLength >= 2 {
		for i := 1; i < policyNamesArrLength; i++ {
			profileID := vcutil.GetSpbmPolicyID(policyNames[i], vs)
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
	gomega.Expect(result.Stdout).To(gomega.Equal("204"))

	// wait for sc to get created in SVC
	for _, policyName := range policyNames {
		err = WaitForScToGetCreated(client, ctx, policyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

}

// createVcSession4RestApis generates session ID for VC to use in rest API calls
func CreateVcSession4RestApis(ctx context.Context, vs *config.E2eTestConfig) string {
	nimbusGeneratedVcPwd := env.GetAndExpectStringEnvVar(constants.VcUIPwd)
	// Read hosts sshd port number
	vcIp, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, vs.TestInput.TestBedInfo.VcAddress)
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
func WaitForScToGetCreated(client clientset.Interface, ctx context.Context, policyName string) error {
	waitErr := wait.PollUntilContextTimeout(ctx, constants.Poll, constants.PollTimeoutShort*5, true,
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

// restartCSIDriver method restarts the csi driver
func ScaleCSIDriver(ctx context.Context, client clientset.Interface, namespace string,
	csiReplicas int32) (bool, error) {
	err := UpdateDeploymentReplicawithWait(client, csiReplicas, constants.VSphereCSIControllerPodNamePrefix,
		constants.CsiSystemNamespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return true, nil
}

// getK8sMasterNodeIPWhereControllerLeaderIsRunning fetches the master node IP
// where controller is running
func GetCSIPodWhereListVolumeResponseIsPresent(ctx context.Context, vs *config.E2eTestConfig,
	client clientset.Interface, sshClientConfig *ssh.ClientConfig,
	containerName string, logMessage string, volumeids []string) (string, string, error) {
	ignoreLabels := make(map[string]string)
	csiControllerPodName, grepCmdForFindingCurrentLeader := "", ""
	csiPods, err := fpod.GetPodsInNamespace(ctx, client, constants.CsiSystemNamespace, ignoreLabels)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var k8sMasterIP string
	if vs.TestInput.ClusterFlavor.VanillaCluster {
		k8sMasterIPs := GetK8sMasterIPs(ctx, client)
		k8sMasterIP = k8sMasterIPs[0]
	} else {
		k8sMasterIP = env.GetAndExpectStringEnvVar(constants.SvcMasterIP)
	}

	for _, csiPod := range csiPods {
		if strings.Contains(csiPod.Name, constants.VSphereCSIControllerPodNamePrefix) {
			// Putting the grepped logs for leader of container of different CSI pods
			// to same temporary file
			// NOTE: This is not valid for vsphere-csi-controller container as for
			// vsphere-csi-controller all the replicas will behave as leaders
			grepCmdForFindingCurrentLeader = "echo `kubectl logs " + csiPod.Name + " -n " +
				constants.CsiSystemNamespace + " " + containerName + " | grep " + "'" + logMessage + "'| " +
				"tail -2` | tee -a listVolumeResponse.log"

			framework.Logf("Invoking command '%v' on host %v", grepCmdForFindingCurrentLeader,
				k8sMasterIP)
			result, err := SshExec(sshClientConfig, vs, k8sMasterIP,
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
	result, err := SshExec(sshClientConfig, vs, k8sMasterIP, cmd)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		return "", "", fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, k8sMasterIP, err)
	}

	return csiControllerPodName, k8sMasterIP, nil
}

// Get all PVC list in the given namespace
func GetAllPVCFromNamespace(client clientset.Interface, namespace string) *v1.PersistentVolumeClaimList {
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

// VerifyIOOnRawBlockVolume helps check data integrity for raw block volumes
func VerifyIOOnRawBlockVolume(ns string, podName string, devicePath string, testdataFile string,
	startSizeInMB, dataSizeInMB int64) {
	// Write some data to file first and then to raw block device
	WriteDataOnRawBlockVolume(ns, podName, devicePath, testdataFile, startSizeInMB, dataSizeInMB)
	// Read the data to verify that is it same as what written
	VerifyDataFromRawBlockVolume(ns, podName, devicePath, testdataFile, startSizeInMB, dataSizeInMB)
}

// WriteDataOnRawBlockVolume writes test data to raw block device
func WriteDataOnRawBlockVolume(ns string, podName string, devicePath string, testdataFile string,
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

// VerifyDataFromRawBlockVolume reads data from raw block device and verifies it against given input
func VerifyDataFromRawBlockVolume(ns string, podName string, devicePath string, testdataFile string,
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

// GetBlockDevSizeInBytes returns size of block device at given path
func GetBlockDevSizeInBytes(f *framework.Framework, ns string, pod *v1.Pod, devicePath string) (int64, error) {
	cmd := []string{"exec", pod.Name, "--namespace=" + ns, "--", "/bin/sh", "-c",
		fmt.Sprintf("/bin/blockdev --getsize64 %v", devicePath)}
	output, err := e2ekubectl.RunKubectl(ns, cmd...)
	if err != nil {
		return -1, fmt.Errorf("failed to get size of raw device %v inside pod", devicePath)
	}
	output = strings.TrimSuffix(output, "\n")
	return strconv.ParseInt(output, 10, 64)
}

// CheckClusterIdValueOnWorkloads checks clusterId value by querying cns metadata
// for all k8s workloads in a particular namespace
func CheckClusterIdValueOnWorkloads(vs *config.E2eTestConfig, client clientset.Interface,
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
		queryResult, err := vcutil.QueryCNSVolumeWithResult(vs, volumeID)
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

// createVsanDPvcAndPod is a wrapper method which creates vsand pvc and pod on svc master IP
func CreateVsanDPvcAndPod(sshClientConfig *ssh.ClientConfig,
	vs *config.E2eTestConfig, svcMasterIP string, svcNamespace string,
	pvcName string, podName string, storagePolicyName string, pvcSize string) error {
	err := ApplyVsanDirectPvcYaml(sshClientConfig, vs, svcMasterIP,
		svcNamespace, pvcName, podName, storagePolicyName, pvcSize)
	if err != nil {
		return err
	}
	err = ApplyVsanDirectPodYaml(sshClientConfig, vs, svcMasterIP, svcNamespace, pvcName, podName)
	if err != nil {
		return err
	}
	return nil
}

// applyVsanDirectPvcYaml creates specific pvc spec and applies vsan direct pvc  yaml on svc master
func ApplyVsanDirectPvcYaml(sshClientConfig *ssh.ClientConfig, vs *config.E2eTestConfig,
	svcMasterIP string, svcNamespace string, pvcName string, podName string,
	storagePolicyName string, pvcSize string) error {

	if pvcSize == "" {
		pvcSize = constants.DiskSize
	}
	cmd := fmt.Sprintf("sed -r"+
		" -i 's/^(\\s*)(namespace\\s*:\\s*.*\\s*$)/\\1namespace: %s/' pvc.yaml", svcNamespace)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err := SshExec(sshClientConfig, vs, svcMasterIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = "kubectl apply -f pvc.yaml"
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}
	return nil
}

// applyVsanDirectPodYaml creates specific pod spec and applies vsan direct pod yaml on svc master
func ApplyVsanDirectPodYaml(sshClientConfig *ssh.ClientConfig,
	vs *config.E2eTestConfig, svcMasterIP string, svcNamespace string,
	pvcName string, podName string) error {
	framework.Logf("POD yaML")
	cmd := fmt.Sprintf("sed -i -e "+
		"'/^metadata:/,/namespace:/{/^\\([[:space:]]*namespace: \\).*/s//\\1%s/}' pod.yaml", svcNamespace)
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err := SshExec(sshClientConfig, vs, svcMasterIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
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
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
		cmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmd, svcMasterIP, err)
	}

	cmd = "kubectl apply -f pod.yaml"
	framework.Logf("Invoking command '%v' on host %v", cmd,
		svcMasterIP)
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
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
func WriteDataToMultipleFilesOnPodInParallel(e2eTestConfig *config.E2eTestConfig,
	namespace string, podName string, data string,
	wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	for i := 0; i < 10; i++ {
		ginkgo.By("write to a file in pod")
		filePath := fmt.Sprintf("/mnt/volume1/file%v.txt", i)
		WriteDataOnFileFromPod(namespace, e2eTestConfig, podName, filePath, data)
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
func DumpSvcNsEventsOnTestFailure(client clientset.Interface, namespace string) {
	if !ginkgo.CurrentSpecReport().Failed() {
		return
	}
	DumpEventsInNs(client, namespace)
}

// DumpEventsInNs dumps events from the given namespace
func DumpEventsInNs(client clientset.Interface, namespace string) {
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
func GetAllPodsFromNamespace(ctx context.Context, client clientset.Interface, namespace string) *v1.PodList {
	podList, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(podList).NotTo(gomega.BeNil())

	return podList
}

// Fetches managed object reference for worker node VMs in the cluster
func GetWorkerVmMoRefs(ctx context.Context, vs *config.E2eTestConfig,
	client clientset.Interface) []vim25types.ManagedObjectReference {
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	workervms := []vim25types.ManagedObjectReference{}
	for _, node := range nodes.Items {
		cpvm := false
		for label := range node.Labels {
			if label == constants.ControlPlaneLabel {
				cpvm = true
			}
		}
		if !cpvm {
			workervms = append(workervms, GetHostMoref4K8sNode(ctx, vs, client, &node))
		}
	}
	return workervms
}

// set storagePolicyQuota
func SetStoragePolicyQuota(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string, quota string) {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + constants.StoragePolicyQuota, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq.Spec.Limit.Reset()
	spq.Spec.Limit.Add(resource.MustParse(quota))
	framework.Logf("set quota %s", quota)

	err = cnsOperatorClient.Update(ctx, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// Remove storagePolicy Quota
func RemoveStoragePolicyQuota(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string) {
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + constants.StoragePolicyQuota, Namespace: namespace}, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	increaseLimit := spq.Spec.Limit
	framework.Logf("Present quota Limit  %s", increaseLimit)
	spq.Spec.Limit.Reset()

	err = cnsOperatorClient.Update(ctx, spq)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq = &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + constants.StoragePolicyQuota, Namespace: namespace}, spq)
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
func GetStoragePolicyQuotaForSpecificResourceType(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string, extensionType string, islatebinding bool) (*resource.Quantity, *resource.Quantity) {
	var usedQuota, reservedQuota *resource.Quantity
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + constants.StoragePolicyQuota, Namespace: namespace}, spq)
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
func GetTotalQuotaConsumedByStoragePolicy(ctx context.Context, restClientConfig *rest.Config,
	scName string, namespace string, islatebinding bool) (*resource.Quantity, *resource.Quantity) {

	var usedQuota, reservedQuota *resource.Quantity
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	spq := &storagepolicyv1alpha2.StoragePolicyQuota{}
	err = cnsOperatorClient.Get(ctx,
		pkgtypes.NamespacedName{Name: scName + constants.StoragePolicyQuota, Namespace: namespace}, spq)
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

// Get GetStoragePolicyUsageForSpecificResourceType based on resourceType (i.e., either volume, snapshot, vmservice)
// resourceUsage will be either pvcUsage, vmUsage and snapshotUsage
func GetStoragePolicyUsageForSpecificResourceType(ctx context.Context, restClientConfig *rest.Config,
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
	return usedQuota, reservedQuota
}

func ValidateTotalStoragequota(ctx context.Context, diskSize int64, totalUsedQuotaBefore *resource.Quantity,
	totalUsedQuotaAfter *resource.Quantity) bool {
	var validTotalQuota bool
	validTotalQuota = false

	out := make([]byte, 0, 64)
	result, suffix := totalUsedQuotaBefore.CanonicalizeBytes(out)
	value := string(result)
	quotaBefore, err := strconv.ParseInt(string(value), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaBefore :  %v%s", quotaBefore, string(suffix)))

	result1, suffix1 := totalUsedQuotaAfter.CanonicalizeBytes(out)
	value1 := string(result1)
	quotaAfter, err := strconv.ParseInt(string(value1), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaAfter :  %v%s", quotaAfter, string(suffix1)))

	if string(suffix) != string(suffix1) {
		if string(suffix1) == "Mi" {
			var bytes = quotaBefore
			// Convert to Gi
			//kibibytes := float64(bytes) / 1024
			quotaBefore = int64(bytes) * 1024
			fmt.Printf("Converted quotaBefore to Mi :  %dMi\n", quotaBefore)
		}
	}

	if string(suffix1) == "Gi" {
		var bytes = diskSize
		// Convert to Gi
		//kibibytes := float64(bytes) / 1024
		diskSize = int64(bytes) / 1024
		fmt.Printf("Storagequota size:  %dGi\n", diskSize)
	}

	ginkgo.By(fmt.Sprintf("quotaBefore+diskSize:  %v, quotaAfter : %v",
		quotaBefore+diskSize, quotaAfter))
	ginkgo.By(fmt.Sprintf("diskSize:  %v", diskSize))

	if quotaBefore+diskSize == quotaAfter {
		validTotalQuota = true
		ginkgo.By(fmt.Sprintf("quotaBefore+diskSize:  %v, quotaAfter : %v",
			quotaBefore+diskSize, quotaAfter))
		ginkgo.By(fmt.Sprintf("diskSize:  %v", diskSize))
		ginkgo.By(fmt.Sprintf("validTotalQuota on storagePolicy:  %v", validTotalQuota))

	}
	return validTotalQuota
}

func ValidateTotalStorageQuotaAfterCleanUp(ctx context.Context, diskSize int64,
	totalUsedQuotaBefore *resource.Quantity, totalUsedQuotaAfterCleanup *resource.Quantity) bool {
	var validTotalQuota bool
	validTotalQuota = false

	out := make([]byte, 0, 64)
	result, suffix := totalUsedQuotaBefore.CanonicalizeBytes(out)
	value := string(result)
	quotaBefore, err := strconv.ParseInt(string(value), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaBefore :  %v%s", quotaBefore, string(suffix)))

	result1, suffix1 := totalUsedQuotaAfterCleanup.CanonicalizeBytes(out)
	value1 := string(result1)
	quotaAfter, err := strconv.ParseInt(string(value1), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf(" quotaAfter :  %v%s", quotaAfter, string(suffix1)))

	if string(suffix) == "Gi" {
		var bytes = diskSize
		// Convert to Gi
		//kibibytes := float64(bytes) / 1024
		diskSize = int64(bytes) / 1024
		fmt.Printf("diskSize:  %dGi\n", diskSize)
	}

	quota := quotaBefore - diskSize

	if string(suffix1) == "Gi" {
		var bytes int64 = quota
		// Convert to Gi
		//kibibytes := float64(bytes) / 1024
		quota = int64(bytes) / 1024
		fmt.Printf("value:  %dGi\n", quota)
	}

	fmt.Printf("diskSize %v ", diskSize)
	fmt.Printf("cleanup quota : %v, quotaAfter: %v ", quota, quotaAfter)
	if quota == quotaAfter {
		validTotalQuota = true
		ginkgo.By(fmt.Sprintf("quotaBefore - diskSize: %v, quotaAfter : %v", quota, quotaAfter))
		ginkgo.By(fmt.Sprintf("validTotalQuota on storagePolicy:  %v", validTotalQuota))

	}

	return validTotalQuota
}

// ValidateReservedQuotaAfterCleanUp  after the volume goes to bound state or
// after teast clean up , expected reserved quota should be "0"
func ValidateReservedQuotaAfterCleanUp(ctx context.Context, total_reservedQuota *resource.Quantity,
	policy_reservedQuota *resource.Quantity, storagepolicyUsage_reserved_Quota *resource.Quantity) bool {
	ginkgo.By(fmt.Sprintf("reservedQuota on total storageQuota CR: %v"+
		"storagePolicyQuota CR: %v, storagePolicyUsage CR: %v ",
		total_reservedQuota.String(), policy_reservedQuota.String(), storagepolicyUsage_reserved_Quota.String()))

	//After the clean up it is expected to have reservedQuota to be '0'
	return total_reservedQuota.String() == "0" &&
		policy_reservedQuota.String() == "0" &&
		storagepolicyUsage_reserved_Quota.String() == "0"

}

func ValidateIncreasedQuota(ctx context.Context, diskSize int64, totalUsedQuotaBefore *resource.Quantity,
	totalUsedQuotaAfterexpansion *resource.Quantity) bool {
	var validTotalQuota bool
	validTotalQuota = false

	out := make([]byte, 0, 64)
	result, suffix := totalUsedQuotaBefore.CanonicalizeBytes(out)
	value := string(result)
	quotaBefore, err := strconv.ParseInt(string(value), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf(" quotaBefore :  %v%s", quotaBefore, string(suffix)))

	result1, suffix1 := totalUsedQuotaAfterexpansion.CanonicalizeBytes(out)
	value1 := string(result1)
	quotaAfter, err := strconv.ParseInt(string(value1), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf(" quotaAfter :  %v%s", quotaAfter, string(suffix1)))

	if string(suffix) == "Gi" {
		var bytes int64 = diskSize
		// Convert to Gi
		diskSize = int64(bytes) / 1024
		fmt.Printf("diskSize:  %dGi\n", diskSize)

	}

	if quotaBefore < quotaAfter {
		validTotalQuota = true
		ginkgo.By(fmt.Sprintf("quotaBefore +diskSize:  %v, quotaAfter : %v", quotaBefore, quotaAfter))
		ginkgo.By(fmt.Sprintf("validTotalQuota on storagePolicy:  %v", validTotalQuota))

	}
	return validTotalQuota
}

// StopKubeSystemPods function stops storageQuotaWebhook POD in kube-system namespace
func StopStorageQuotaWebhookPodInKubeSystem(ctx context.Context, client clientset.Interface,
	namespace string) (bool, error) {
	CollectPodLogs(ctx, client, constants.KubeSystemNamespace)
	isServiceStopped := false
	err := UpdateDeploymentReplicawithWait(client, 0, constants.StorageQuotaWebhookPrefix,
		namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	isServiceStopped = true
	return isServiceStopped, err
}

// startCSIPods function starts the csi pods and waits till all the pods comes up
func StartStorageQuotaWebhookPodInKubeSystem(ctx context.Context, client clientset.Interface, csiReplicas int32,
	namespace string) (bool, error) {
	ignoreLabels := make(map[string]string)
	err := UpdateDeploymentReplicawithWait(client, csiReplicas, constants.StorageQuotaWebhookPrefix,
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
		constants.PollTimeout)
	isServiceStopped := false
	return isServiceStopped, err
}

// getStoragePolicyUsedAndReservedQuotaDetails This method returns the used and reserved quota's from
// storageQuota CR, storagePolicyQuota CR and storagePolicyUsage CR
func GetStoragePolicyUsedAndReservedQuotaDetails(ctx context.Context, restConfig *rest.Config,
	storagePolicyName string, namespace string, resourceUsageName string, resourceExtensionName string,
	islatebinding bool) (*resource.Quantity, *resource.Quantity,
	*resource.Quantity, *resource.Quantity, *resource.Quantity, *resource.Quantity) {

	var totalQuotaUsed, totalQuotaReserved, storagePolicyQuotaCRUsed, storagePolicyQuotaCRReserved *resource.Quantity
	var storagePolicyUsageCRUsed, storagePolicyUsageCRReserved *resource.Quantity

	totalQuotaUsed, totalQuotaReserved = GetTotalQuotaConsumedByStoragePolicy(ctx, restConfig,
		storagePolicyName, namespace, islatebinding)
	framework.Logf("Namespace total-Quota-Used: %s,total-Quota-Reserved: %s ", totalQuotaUsed, totalQuotaReserved)

	if resourceExtensionName != "" {
		framework.Logf("**resourceUsageName: %s, resourceExtensionName: %s **", resourceUsageName,
			resourceExtensionName)
		storagePolicyQuotaCRUsed, storagePolicyQuotaCRReserved = GetStoragePolicyQuotaForSpecificResourceType(ctx,
			restConfig, storagePolicyName, namespace, resourceExtensionName, islatebinding)
		framework.Logf("Policy-Quota-CR-Used: %s, Policy-Quota-CR-Reserved: %s", storagePolicyQuotaCRUsed,
			storagePolicyQuotaCRReserved)
	} else {
		storagePolicyQuotaCRUsed = nil
		storagePolicyQuotaCRReserved = nil
	}

	if resourceUsageName != "" {
		if islatebinding {
			storagePolicyUsageCRUsed, storagePolicyUsageCRReserved = GetStoragePolicyUsageForSpecificResourceType(ctx,
				restConfig, storagePolicyName+"-latebinding", namespace, resourceUsageName)
		} else {
			storagePolicyUsageCRUsed, storagePolicyUsageCRReserved = GetStoragePolicyUsageForSpecificResourceType(ctx,
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
func ValidateQuotaUsageAfterResourceCreation(ctx context.Context, restConfig *rest.Config, storagePolicyName string,
	namespace string, resourceUsage string, resourceExtensionName string, size []string,
	totalQuotaUsedBefore *resource.Quantity, storagePolicyQuotaBefore *resource.Quantity,
	storagePolicyUsageBefore *resource.Quantity, islatebinding bool) (bool, bool) {

	_, _, storagePolicyQuotaAfter, _, storagePolicyUsageAfter, _ :=
		GetStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
			storagePolicyName, namespace, resourceUsage, resourceExtensionName, islatebinding)

	sp_quota_validation := Validate_totalStoragequota(ctx, size, storagePolicyQuotaBefore,
		storagePolicyQuotaAfter)
	framework.Logf("Storage-policy-Quota CR validation status :%v", sp_quota_validation)

	sp_usage_validation := Validate_totalStoragequota(ctx, size, storagePolicyUsageBefore,
		storagePolicyUsageAfter)
	framework.Logf("Storage-policy-usage CR validation status :%v", sp_usage_validation)

	return sp_quota_validation, sp_usage_validation
}

// validateQuotaUsageAfterCleanUp verifies the storagequota details shows up on all CR's after resource cleanup
func ValidateQuotaUsageAfterCleanUp(ctx context.Context, restConfig *rest.Config, storagePolicyName string,
	namespace string, resourceUsage string, resourceExtensionName string, diskSizeInMb string,
	totalQuotaUsedAfter *resource.Quantity, storagePolicyQuotaAfter *resource.Quantity,
	storagePolicyUsageAfter *resource.Quantity, islatebinding bool) {

	totalQuotaUsedCleanup, totalQuotaReserved, storagePolicyQuotaAfterCleanup, storagePolicyQuotaReserved,
		storagePolicyUsageAfterCleanup, storagePolicyUsageReserved := GetStoragePolicyUsedAndReservedQuotaDetails(
		ctx, restConfig, storagePolicyName, namespace, resourceUsage, resourceExtensionName, islatebinding)

	quotavalidationStatusAfterCleanup := Validate_totalStoragequota_afterCleanUp(ctx, diskSizeInMb,
		totalQuotaUsedAfter, totalQuotaUsedCleanup)
	gomega.Expect(quotavalidationStatusAfterCleanup).NotTo(gomega.BeFalse())

	quotavalidationStatusAfterCleanup = Validate_totalStoragequota_afterCleanUp(ctx, diskSizeInMb,
		storagePolicyQuotaAfter, storagePolicyQuotaAfterCleanup)
	gomega.Expect(quotavalidationStatusAfterCleanup).NotTo(gomega.BeFalse())

	quotavalidationStatusAfterCleanup = Validate_totalStoragequota_afterCleanUp(ctx, diskSizeInMb,
		storagePolicyUsageAfter, storagePolicyUsageAfterCleanup)
	gomega.Expect(quotavalidationStatusAfterCleanup).NotTo(gomega.BeFalse())

	reservedQuota := Validate_reservedQuota_afterCleanUp(ctx, totalQuotaReserved,
		storagePolicyQuotaReserved, storagePolicyUsageReserved)
	gomega.Expect(reservedQuota).NotTo(gomega.BeFalse())

	framework.Logf("quotavalidationStatus :%v reservedQuota:%v", quotavalidationStatusAfterCleanup,
		reservedQuota)
}

// ExecCommandOnGcWorker logs into gc worker node using ssh private key and executes command
func ExecCommandOnGcWorker(sshClientConfig *ssh.ClientConfig,
	vs *config.E2eTestConfig, svcMasterIP string, gcWorkerIp string,
	svcNamespace string, cmd string) (fssh.Result, error) {
	result := fssh.Result{Host: gcWorkerIp, Cmd: cmd}
	// get the cluster ssh key
	sshSecretName := env.GetAndExpectStringEnvVar(constants.SshSecretName)
	cmdToGetPrivateKey := fmt.Sprintf("kubectl get secret %s -n %s -o"+
		"jsonpath={'.constants.ssh-privatekey'} | base64 -d > key", sshSecretName, svcNamespace)
	framework.Logf("Invoking command '%v' on host %v", cmdToGetPrivateKey,
		svcMasterIP)
	cmdResult, err := SshExec(sshClientConfig, vs, svcMasterIP,
		cmdToGetPrivateKey)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return result, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmdToGetPrivateKey, svcMasterIP, err)
	}

	enablePermissionCmd := "chmod 600 key"
	framework.Logf("Invoking command '%v' on host %v", enablePermissionCmd,
		svcMasterIP)
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
		enablePermissionCmd)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return result, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			enablePermissionCmd, svcMasterIP, err)
	}

	cmdToGetContainerInfo := fmt.Sprintf("ssh -o StrictHostKeyChecking=no -i key %s@%s "+
		"'%s' 2> /dev/null", constants.GcNodeUser, gcWorkerIp, cmd)
	framework.Logf("Invoking command '%v' on host %v", cmdToGetContainerInfo,
		svcMasterIP)
	cmdResult, err = SshExec(sshClientConfig, vs, svcMasterIP,
		cmdToGetContainerInfo)
	if err != nil || cmdResult.Code != 0 {
		fssh.LogResult(cmdResult)
		return result, fmt.Errorf("couldn't execute command: %s on host: %v , error: %s",
			cmdToGetContainerInfo, svcMasterIP, err)
	}
	return cmdResult, nil
}

// expandPVCSize expands PVC size
func ExpandPVCSize(origPVC *v1.PersistentVolumeClaim, size resource.Quantity,
	c clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvcName := origPVC.Name
	updatedPVC := origPVC.DeepCopy()

	waitErr := wait.PollUntilContextTimeout(ctx, constants.ResizePollInterval, 30*time.Second, true,
		func(ctx context.Context) (bool, error) {
			var err error
			updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("error fetching pvc %q for resizing with %v", pvcName, err)
			}

			updatedPVC.Spec.Resources.Requests[v1.ResourceStorage] = size
			updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Update(
				ctx, updatedPVC, metav1.UpdateOptions{})
			if err == nil {
				return true, nil
			}
			framework.Logf("Error updating pvc %s with %v", pvcName, err)
			return false, nil
		})
	return updatedPVC, waitErr
}

// ExpandVolumeInParallel resizes a list of volumes to a new size in parallel
func ExpandVolumeInParallel(client clientset.Interface, pvclaims []*v1.PersistentVolumeClaim,
	wg *sync.WaitGroup, resizeValue string) {

	defer wg.Done()
	for _, pvclaim := range pvclaims {
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse(resizeValue))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err := ExpandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}
	}
}

/*
Helper method to verify the status code
*/
func CheckStatusCode(expectedStatusCode int, actualStatusCode int) error {
	gomega.Expect(actualStatusCode).Should(gomega.BeNumerically("==", expectedStatusCode))
	if actualStatusCode != expectedStatusCode {
		return fmt.Errorf("expected status code: %d, actual status code: %d", expectedStatusCode, actualStatusCode)
	}
	return nil
}

/*
This helper is to check if a given integer present in a list of intergers.
*/
func IsAvailable(alpha []int, val int) bool {
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
This util will fetch and compare the storage policy usage CR created for each storage class for a namespace
*/
func ListStoragePolicyUsages(ctx context.Context, c clientset.Interface, restClientConfig *rest.Config,
	namespace string, storageclass []string) {

	// Build expected usage names directly from passed storageclass names
	expectedUsages := make(map[string]bool)
	for _, sc := range storageclass {
		for _, suffix := range constants.UsageSuffixes {
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
	waitErr := wait.PollUntilContextTimeout(ctx, constants.StoragePolicyUsagePollInterval,
		constants.StoragePolicyUsagePollTimeout,
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

// createPVCAndQueryVolumeInCNS creates PVc with a given storage class on a given namespace
// and verifies cns metadata of that volume if verifyCNSVolume is set to true
func CreatePVCAndQueryVolumeInCNS(ctx context.Context, client clientset.Interface,
	vs *config.E2eTestConfig, namespace string,
	pvclaimLabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
	ds string, storageclass *storagev1.StorageClass,
	verifyCNSVolume bool) (*v1.PersistentVolumeClaim, []*v1.PersistentVolume, error) {

	// Create PVC
	pvclaim, err := CreatePVC(ctx, client, namespace, pvclaimLabels, ds, storageclass, accessMode)
	if err != nil {
		return pvclaim, nil, fmt.Errorf("failed to create PVC: %w", err)
	}

	// Wait for PVC to be bound to a PV
	persistentvolumes, err := WaitForPVClaimBoundPhase(ctx, client, vs,
		[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout*2)
	if err != nil {
		return pvclaim, persistentvolumes, fmt.Errorf("failed to wait for PVC to bind to a PV: %w", err)
	}

	// Get VolumeHandle from the PV
	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle
	if vs.TestInput.ClusterFlavor.GuestCluster {
		volHandle = GetVolumeIDFromSupervisorCluster(volHandle)
	}
	if volHandle == "" {
		return pvclaim, persistentvolumes, fmt.Errorf("volume handle is empty")
	}

	// Verify the volume in CNS if required
	if verifyCNSVolume {
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := vcutil.QueryCNSVolumeWithResult(vs, volHandle)
		if err != nil {
			return pvclaim, persistentvolumes, fmt.Errorf("failed to query CNS volume: %w", err)
		}
		if len(queryResult.Volumes) == 0 || queryResult.Volumes[0].VolumeId.Id != volHandle {
			return pvclaim, persistentvolumes, fmt.Errorf("CNS query returned unexpected result")
		}
	}

	return pvclaim, persistentvolumes, nil
}

// waitForPvResizeForGivenPvc waits for the controller resize to be finished
func WaitForPvResizeForGivenPvc(pvc *v1.PersistentVolumeClaim, c clientset.Interface,
	vs *config.E2eTestConfig, duration time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adminClient, _ := InitializeClusterClientsByUserRoles(c, vs)
	pvName := pvc.Spec.VolumeName
	pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pv, err := adminClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return WaitForPvResize(pv, c, vs, pvcSize, duration)
}

// waitForPvResize waits for the controller resize to be finished
func WaitForPvResize(pv *v1.PersistentVolume, c clientset.Interface, vs *config.E2eTestConfig,
	size resource.Quantity, duration time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, _ := InitializeClusterClientsByUserRoles(c, vs)
	return wait.PollUntilContextTimeout(ctx, constants.ResizePollInterval, duration, true,
		func(ctx context.Context) (bool, error) {
			pv, err := adminClient.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})

			if err != nil {
				return false, fmt.Errorf("error fetching pv %q for resizing %v", pv.Name, err)
			}

			pvSize := pv.Spec.Capacity[v1.ResourceStorage]

			// If pv size is greater or equal to requested size that means controller resize is finished.
			if pvSize.Cmp(size) >= 0 {
				return true, nil
			}
			return false, nil
		})
}

// WaitForPVClaimBoundPhase waits until all pvcs phase set to bound
// client: framework generated client
// pvclaims: list of PVCs
// timeout: timeInterval to wait for PVCs to get into bound state
// ctx: context package variable
func WaitForPVClaimBoundPhase(ctx context.Context, client clientset.Interface, vs *config.E2eTestConfig,
	pvclaims []*v1.PersistentVolumeClaim, timeout time.Duration) ([]*v1.PersistentVolume, error) {
	persistentvolumes := make([]*v1.PersistentVolume, len(pvclaims))

	adminClient, _ := InitializeClusterClientsByUserRoles(client, vs)
	for index, claim := range pvclaims {
		err := fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, adminClient,
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
func CreateScopedClient(ctx context.Context, client clientset.Interface,
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
				Kind:      constants.ServiceAccountKeyword,
				Name:      saName,
				Namespace: ns,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     constants.RoleKeyword,
			Name:     roleName,
			APIGroup: constants.RbacApiGroup,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create RoleBinding: %v", err)
	}

	var token string

	tr := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{constants.AudienceForSvcAccountName},
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
	localPort := env.GetAndExpectStringEnvVar("RANDOM_PORT")
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
func InitializeClusterClientsByUserRoles(client clientset.Interface, vs *config.E2eTestConfig) (clientset.Interface,
	clientset.Interface) {
	var adminClient clientset.Interface
	var err error
	runningAsDevopsUser := env.GetBoolEnvVarOrDefault(constants.EnvIsDevopsUser, false)
	if vs.TestInput.ClusterFlavor.SupervisorCluster || vs.TestInput.ClusterFlavor.GuestCluster {
		if runningAsDevopsUser {
			if svAdminK8sEnv := env.GetAndExpectStringEnvVar(constants.EnvAdminKubeconfig); svAdminK8sEnv != "" {
				adminClient, err = CreateKubernetesClientFromConfig(svAdminK8sEnv)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if vs.TestInput.ClusterFlavor.SupervisorCluster {
				if devopsK8sEnv := env.GetAndExpectStringEnvVar(constants.EnvDevopsKubeconfig); devopsK8sEnv != "" {
					client, err = CreateKubernetesClientFromConfig(devopsK8sEnv)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		} else {
			adminClient = client
		}
	} else if vs.TestInput.ClusterFlavor.VanillaCluster || adminClient == nil {
		adminClient = client
	}
	return adminClient, client
}

/*
CreateStatefulSetAndVerifyPVAndPodNodeAffinty creates user specified statefulset and
further checks the node and volumes affinities
*/
func CreateStatefulSetAndVerifyPVAndPodNodeAffinty(ctx context.Context, client clientset.Interface,
	vs *config.E2eTestConfig, namespace string, parallelPodPolicy bool, replicas int32, nodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	podAntiAffinityToSet bool, parallelStatefulSetCreation bool, modifyStsSpec bool,
	accessMode v1.PersistentVolumeAccessMode,
	sc *storagev1.StorageClass, verifyTopologyAffinity bool, storagePolicy string) (*v1.Service,
	*appsv1.StatefulSet, error) {

	ginkgo.By("Create service")
	service := CreateService(namespace, client)

	framework.Logf("Create StatefulSet")
	statefulset := CreateCustomisedStatefulSets(ctx, client, vs.TestInput, namespace, parallelPodPolicy,
		replicas, nodeAffinityToSet, allowedTopologies, podAntiAffinityToSet, modifyStsSpec,
		"", accessMode, sc, storagePolicy)

	if verifyTopologyAffinity {
		framework.Logf("Verify PV node affinity and that the PODS are running on appropriate node")
		err := VerifyPVnodeAffinityAndPODnodedetailsForStatefulsetsLevel5(ctx, vs, client, statefulset,
			namespace, allowedTopologies, parallelStatefulSetCreation)
		if err != nil {
			return nil, nil, fmt.Errorf("error verifying PV node affinity and POD node details: %v", err)
		}
	}

	return service, statefulset, nil
}

/*
createCustomisedStatefulSets util methods creates statefulset as per the user's
specific requirement and returns the customised statefulset
*/
func CreateCustomisedStatefulSets(ctx context.Context, client clientset.Interface, vs *config.TestInputData,
	namespace string, isParallelPodMgmtPolicy bool, replicas int32, nodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement,
	podAntiAffinityToSet bool, modifyStsSpec bool, stsName string,
	accessMode v1.PersistentVolumeAccessMode, sc *storagev1.StorageClass, storagePolicy string) *appsv1.StatefulSet {
	framework.Logf("Preparing StatefulSet Spec")
	statefulset := GetStatefulSetFromManifest(vs, namespace)

	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		defaultAccessMode := v1.ReadWriteOnce
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.AccessModes[0] = defaultAccessMode
	} else {
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
			accessMode
	}

	if modifyStsSpec {
		if vs.TestBedInfo.MultipleSvc {
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &storagePolicy
		} else {
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &sc.Name
		}

		if stsName != "" {
			statefulset.Name = stsName
			statefulset.Spec.Template.Labels["app"] = statefulset.Name
			statefulset.Spec.Selector.MatchLabels["app"] = statefulset.Name
		}

	}
	if nodeAffinityToSet {
		nodeSelectorTerms := GetNodeSelectorTerms(allowedTopologies)
		statefulset.Spec.Template.Spec.Affinity = new(v1.Affinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity = new(v1.NodeAffinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution = new(v1.NodeSelector)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = nodeSelectorTerms
	}
	if podAntiAffinityToSet {
		statefulset.Spec.Template.Spec.Affinity = &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"key": "app",
							},
						},
						TopologyKey: "topology.kubernetes.io/zone",
					},
				},
			},
		}

	}
	if isParallelPodMgmtPolicy {
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
	}
	statefulset.Spec.Replicas = &replicas

	framework.Logf("Creating statefulset")
	CreateStatefulSet(namespace, statefulset, client)

	framework.Logf("Wait for StatefulSet pods to be in up and running state")
	fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
	gomega.Expect(fss.CheckMount(ctx, client, statefulset, constants.MountPath)).NotTo(gomega.HaveOccurred())
	ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset should match with number of replicas")

	return statefulset
}

// CreateStatefulSet creates a StatefulSet from the manifest at manifestPath in the given namespace.
func CreateStatefulSet(ns string, ss *appsv1.StatefulSet, c clientset.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf("Creating statefulset %v/%v with %d replicas and selector %+v",
		ss.Namespace, ss.Name, *(ss.Spec.Replicas), ss.Spec.Selector)
	_, err := c.AppsV1().StatefulSets(ns).Create(ctx, ss, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	fss.WaitForRunningAndReady(ctx, c, *ss.Spec.Replicas, ss)
}

func Validate_totalStoragequota_afterCleanUp(ctx context.Context, diskSize string,
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
func Validate_reservedQuota_afterCleanUp(ctx context.Context, total_reservedQuota *resource.Quantity,
	policy_reservedQuota *resource.Quantity, storagepolicyUsage_reserved_Quota *resource.Quantity) bool {
	ginkgo.By(fmt.Sprintf("reservedQuota on total storageQuota CR: %v"+
		"storagePolicyQuota CR: %v, storagePolicyUsage CR: %v ",
		total_reservedQuota.String(), policy_reservedQuota.String(), storagepolicyUsage_reserved_Quota.String()))

	//After the clean up it is expected to have reservedQuota to be '0'
	return total_reservedQuota.String() == "0" &&
		policy_reservedQuota.String() == "0" &&
		storagepolicyUsage_reserved_Quota.String() == "0"

}

func Validate_totalStoragequota(ctx context.Context, diskSizes []string, totalUsedQuotaBefore *resource.Quantity,
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

	totalDiskStorage = SumupAlltheResourceDiskUsage(diskSizes, diskunit_quotaAfter)

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

// Convert all the disks storage unit to the expected unit value and Sums up and returns the totalDiskStorage
func SumupAlltheResourceDiskUsage(diskSizes []string, expectedUnit []byte) int64 {
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

/*
This function creates a wcp namespace in a vSphere supervisor Cluster, associating it
with multiple storage policies and zones.
It constructs an API request and sends it to the vSphere REST API.
*/
func CreatetWcpNsWithZonesAndPolicies(e2eTestConfig *config.E2eTestConfig,
	vcRestSessionId string, storagePolicyId []string,
	supervisorId string, zoneNames []string,
	vmClass string, contentLibId string) (string, int, error) {

	r := rand.New(rand.NewSource(time.Now().Unix()))
	namespace := fmt.Sprintf("csi-%v", r.Intn(10000))
	initailUrl := CreateInitialNsApiCallUrl(e2eTestConfig)
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
	_, statusCode := InvokeVCRestAPIPostRequest(vcRestSessionId, nsCreationUrl, reqBody)

	return namespace, statusCode, nil
}

// invokeVCRestAPIPostRequest invokes POST on given VC REST URL using the passed session token and request body
func InvokeVCRestAPIPostRequest(vcRestSessionId string, url string, reqBody string) ([]byte, int) {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: transCfg}
	framework.Logf("Invoking POST on url: %s", url)
	req, err := http.NewRequest("POST", url, strings.NewReader(reqBody))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add(constants.VcRestSessionIdHeaderName, vcRestSessionId)
	req.Header.Add("Content-type", "application/json")

	resp, statusCode := HttpRequest(httpClient, req)

	return resp, statusCode
}

/*
This util will create initial namespace get/post api call request
*/
func CreateInitialNsApiCallUrl(e2eTestConfig *config.E2eTestConfig) string {
	vcIp := e2eTestConfig.TestInput.Global.VCenterHostname

	isPrivateNetwork := env.GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vcIp = env.GetStringEnvVarOrDefault("LOCAL_HOST_IP", constants.DefaultlocalhostIP)
	}

	initialUrl := "https://" + vcIp + ":" + e2eTestConfig.TestInput.Global.VCenterPort +
		"/api/vcenter/namespaces/instances/"

	return initialUrl
}

// getSvcId fetches the ID of the Supervisor cluster
func GetSvcId(vcRestSessionId string, e2eTestConfig *config.E2eTestConfig) string {

	isPrivateNetwork := env.GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	vCenterIp := e2eTestConfig.TestInput.Global.VCenterHostname
	if isPrivateNetwork {
		vCenterIp = env.GetStringEnvVarOrDefault("LOCAL_HOST_IP", constants.DefaultlocalhostIP)
	}

	svcIdFetchUrl := "https://" + vCenterIp + ":" + e2eTestConfig.TestInput.Global.VCenterPort +
		"/api/vcenter/namespace-management/supervisors/summaries"

	resp, statusCode := InvokeVCRestAPIGetRequest(vcRestSessionId, svcIdFetchUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var v map[string]interface{}
	gomega.Expect(json.Unmarshal(resp, &v)).NotTo(gomega.HaveOccurred())
	framework.Logf("Supervisor summary: %v", v)
	return v["items"].([]interface{})[0].(map[string]interface{})["supervisor"].(string)
}

// invokeVCRestAPIGetRequest invokes GET on given VC REST URL using the passed session token and verifies that the
// return status code is 200
func InvokeVCRestAPIGetRequest(vcRestSessionId string, url string) ([]byte, int) {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: transCfg}
	framework.Logf("Invoking GET on url: %s", url)
	req, err := http.NewRequest("GET", url, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add(constants.VcRestSessionIdHeaderName, vcRestSessionId)

	resp, statusCode := HttpRequest(httpClient, req)

	return resp, statusCode
}

// delTestWcpNs triggeres a wcp namespace deletion asynchronously
func DelTestWcpNs(e2eTestConfig *config.E2eTestConfig, vcRestSessionId string, namespace string) {
	vcIp := e2eTestConfig.TestInput.Global.VCenterHostname
	isPrivateNetwork := env.GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vcIp = env.GetStringEnvVarOrDefault("LOCAL_HOST_IP", constants.DefaultlocalhostIP)
	}
	nsDeletionUrl := "https://" + vcIp + ":" + e2eTestConfig.TestInput.Global.VCenterPort +
		"/api/vcenter/namespaces/instances/" + namespace
	_, statusCode := InvokeVCRestAPIDeleteRequest(vcRestSessionId, nsDeletionUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 204))
	framework.Logf("Successfully Deleted namepsace %v in SVC.", namespace)
}

// invokeVCRestAPIDeleteRequest invokes DELETE on given VC REST URL using the passed session token
func InvokeVCRestAPIDeleteRequest(vcRestSessionId string, url string) ([]byte, int) {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: transCfg}
	framework.Logf("Invoking DELETE on url: %s", url)
	req, err := http.NewRequest("DELETE", url, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add(constants.VcRestSessionIdHeaderName, vcRestSessionId)

	resp, statusCode := HttpRequest(httpClient, req)

	return resp, statusCode
}

// Convert mb to String
func ConvertInt64ToStrMbFormat(diskSize int64) string {
	result := strconv.FormatInt(diskSize, 10) + "Mi"
	fmt.Println(result)
	return result
}

// expectEqual expects the specified two are the same, otherwise an exception raises
func ExpectEqual(actual interface{}, extra interface{}, explain ...interface{}) {
	gomega.ExpectWithOffset(1, actual).To(gomega.Equal(extra), explain...)
}

// GetPersistentVolumeClaimSpecWithDatasource return the PersistentVolumeClaim
// spec with specified storage class.
func GetPersistentVolumeClaimSpecWithDatasource(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode,
	datasourceName string, snapshotapigroup string) *v1.PersistentVolumeClaim {
	disksize := constants.DiskSize
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

// CreatePVCWithPvcSpec helps creates pvc with given namespace and using given pvc spec
func CreatePvcWithSpec(
	ctx context.Context,
	client clientset.Interface,
	pvcnamespace string,
	pvcspec *v1.PersistentVolumeClaim,
) (*v1.PersistentVolumeClaim, error) {
	pvclaim, err := fpv.CreatePVC(ctx, client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create pvc with err: %v", err))
	framework.Logf("PVC created: %v in namespace: %v", pvclaim.Name, pvcnamespace)
	return pvclaim, err
}

// This method gets storageclass and creates resource quota
// Returns restConfig,  storageclass and profileId
// This is used in staticProvisioning for presetup.
func StaticProvisioningPreSetUpUtil(ctx context.Context, vs *config.E2eTestConfig, f *framework.Framework,
	c clientset.Interface, storagePolicyName string, namespace string) (*rest.Config, *storagev1.StorageClass, string) {
	if namespace == "" {
		namespace = vcutil.GetNamespaceToRunTests(f, vs)
	}
	adminClient, _ := InitializeClusterClientsByUserRoles(c, vs)
	// Get a config to talk to the apiserver
	k8senv := env.GetAndExpectStringEnvVar("KUBECONFIG")
	restConfig, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	profileID := vcutil.GetSpbmPolicyID(storagePolicyName, vs)
	framework.Logf("Profile ID :%s", profileID)
	scParameters := make(map[string]string)
	scParameters["storagePolicyID"] = profileID

	if !vs.TestInput.ClusterFlavor.SupervisorCluster {
		err = adminClient.StorageV1().StorageClasses().Delete(ctx, storagePolicyName, metav1.DeleteOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}

	storageclass, err := CreateStorageClass(c, vs, scParameters, nil, "", "", true, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("storageclass Name: %s", storageclass.GetName()))
	storageclass, err = adminClient.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("create resource quota")
	CreateResourceQuota(adminClient, vs, namespace, constants.RqLimit, storagePolicyName)

	return restConfig, storageclass, profileID
}
