/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/pkg/sftp"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmopv3common "github.com/vmware-tanzu/vm-operator/api/v1alpha3/common"
	vmopv4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	vmopv4common "github.com/vmware-tanzu/vm-operator/api/v1alpha4/common"
	vmopv5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
)

type subscribedContentLibBasic struct {
	Id      string
	name    string
	ds_moId string
	url     string
}

const vmServiceVmLabelKey = "topology.kubernetes.io/zone"

// createTestWcpNs create a wcp namespace with given storage policy, vm class and content lib via REST API
func createTestWcpNs(
	vcRestSessionId string, storagePolicyId string, vmClass string, contentLibId string,
	supervisorId string) string {

	vcIp := e2eVSphere.Config.Global.VCenterHostname
	r := rand.New(rand.NewSource(time.Now().Unix()))

	namespace := fmt.Sprintf("csi-vmsvcns-%v", r.Intn(10000))

	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vcIp = GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)
	}

	nsCreationUrl := "https://" + vcIp + ":" + e2eVSphere.Config.Global.VCenterPort +
		"/api/vcenter/namespaces/instances/v2"
	reqBody := fmt.Sprintf(`{
        "namespace": "%s",
        "storage_specs": [  {
            "policy": "%s"
        } ],
        "vm_service_spec":  {
            "vm_classes": [
                "%s"
            ],
            "content_libraries": [
                "%s"
            ]
        },
        "supervisor": "%s"
    }`, namespace, storagePolicyId, vmClass, contentLibId, supervisorId)

	fmt.Println(reqBody)

	_, statusCode := invokeVCRestAPIPostRequest(vcRestSessionId, nsCreationUrl, reqBody)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 204))
	framework.Logf("Successfully created namepsace %v in SVC.", namespace)
	return namespace
}

// delTestWcpNs triggeres a wcp namespace deletion asynchronously
func delTestWcpNs(vcRestSessionId string, namespace string) {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vcIp = GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)
	}
	nsDeletionUrl := "https://" + vcIp + ":" + e2eVSphere.Config.Global.VCenterPort +
		"/api/vcenter/namespaces/instances/" + namespace
	_, statusCode := invokeVCRestAPIDeleteRequest(vcRestSessionId, nsDeletionUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 204))
	framework.Logf("Successfully Deleted namepsace %v in SVC.", namespace)
}

// getSvcId fetches the ID of the Supervisor cluster
func getSvcId(vcRestSessionId string, vs *vSphere) string {

	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	vCenterIp := vs.Config.Global.VCenterHostname
	if isPrivateNetwork {
		vCenterIp = GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)
	}

	svcIdFetchUrl := "https://" + vCenterIp + ":" + vs.Config.Global.VCenterPort +
		"/api/vcenter/namespace-management/supervisors/summaries"

	resp, statusCode := invokeVCRestAPIGetRequest(vcRestSessionId, svcIdFetchUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var v map[string]interface{}
	gomega.Expect(json.Unmarshal(resp, &v)).NotTo(gomega.HaveOccurred())
	framework.Logf("Supervisor summary: %v", v)
	return v["items"].([]interface{})[0].(map[string]interface{})["supervisor"].(string)
}

// createAndOrGetContentlibId4Url fetches ID of a content lib that matches the given URL, if none are found it creates a
// new content lib with the given URL and returns its ID
func createAndOrGetContentlibId4Url(vcRestSessionId string, contentLibUrl string, dsMoId string,
	vs *vSphere) (string, error) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	contentlibName := fmt.Sprintf("csi-vmsvc-%v", r.Intn(10000))

	// Try to get the existing Content Library ID
	contentLibId, err := getContentLibId4Url(vcRestSessionId, contentLibUrl, vs)
	if err == nil {
		if contentLibId == "" {
			return "", fmt.Errorf("existing content library ID is empty")
		}
		return contentLibId, nil
	}

	// Get SSL Thumbprint
	sslThumbPrint, err := getSslThumbprintForContentLibraryCreation(vcRestSessionId,
		contentLibUrl, &e2eVSphere)
	if err != nil {
		return "", fmt.Errorf("failed to get SSL thumbprint: %w", err)
	}

	vcIp := e2eVSphere.Config.Global.VCenterHostname
	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vcIp = GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)
	}
	contentlbCreationUrl := "https://" + vcIp + ":" + vs.Config.Global.VCenterPort +
		"/api/content/subscribed-library"
	reqBody := fmt.Sprintf(`{
        "name": "%s",
        "storage_backings": [{
            "datastore_id": "%s",
            "type": "DATASTORE"
        }],
        "subscription_info": {
            "authentication_method": "NONE",
            "automatic_sync_enabled": true,
            "on_demand": true,
            "subscription_url": "%s",
            "ssl_thumbprint": "%s"
        },
        "type": "SUBSCRIBED"
    }`, contentlibName, dsMoId, contentLibUrl, sslThumbPrint)

	resp, statusCode := invokeVCRestAPIPostRequest(vcRestSessionId, contentlbCreationUrl, reqBody)
	if statusCode != 201 {
		return "", fmt.Errorf("API call failed with status code %d", statusCode)
	}

	// Unmarshal response to get Content Library ID
	if err := json.Unmarshal(resp, &contentLibId); err != nil {
		return "", fmt.Errorf("failed to parse response JSON: %w", err)
	}

	/* Check if the content library ID is empty after creation, as a successful API
	call (201) may still result in an empty ID */
	if contentLibId == "" {
		return "", fmt.Errorf("content library ID is empty after creation")
	}

	framework.Logf("Successfully created content library %s for the test (id: %v)", contentlibName, contentLibId)
	return contentLibId, nil
}

/*
getSslThumbprintForContentLibraryCreation util will fetch the thumbprint
required to create a content library
*/
func getSslThumbprintForContentLibraryCreation(vcRestSessionId string, contentLibUrl string,
	vs *vSphere) (string, error) {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vcIp = GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)
	}

	contentlbCreationUrl := "https://" + vcIp + ":" + vs.Config.Global.VCenterPort +
		"/api/content/subscribed-library?action=probe"

	reqBody := fmt.Sprintf(`{
        "subscription_info": {
            "subscription_url": "%s"
        }
    }`, contentLibUrl)

	resp, statusCode := invokeVCRestAPIPostRequest(vcRestSessionId, contentlbCreationUrl, reqBody)

	if statusCode == 200 {
		var responseData map[string]interface{}
		if err := json.Unmarshal(resp, &responseData); err != nil {
			return "", fmt.Errorf("failed to parse response JSON: %w", err)
		}

		if sslThumbprint, ok := responseData["ssl_thumbprint"].(string); ok {
			fmt.Println("SSL Thumbprint:", sslThumbprint)
			return sslThumbprint, nil
		}

		return "", fmt.Errorf("ssl_thumbprint not found in response")
	}

	return "", fmt.Errorf("API call failed with status code: %d", statusCode)
}

// getContentLibId4Url fetches ID of a content lib that matches the given URL
func getContentLibId4Url(vcRestSessionId string, url string, vs *vSphere) (string, error) {
	var libId string
	libIds := getAllContentLibIds(vcRestSessionId, vs)
	for _, libId := range libIds {
		lib := getContentLib(vcRestSessionId, libId, vs)
		if lib.url == url {
			return libId, nil
		}
	}
	return libId, fmt.Errorf("couldn't find a content lib with subscription url '%v'", url)
}

// getAllContentLibIds fetches IDs of all content libs
func getAllContentLibIds(vcRestSessionId string, vs *vSphere) []string {
	vCenterIp := e2eVSphere.Config.Global.VCenterHostname

	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vCenterIp = GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)
	}
	contentLibsFetchUrl := "https://" + vCenterIp + ":" + vs.Config.Global.VCenterPort +
		"/api/content/subscribed-library"

	resp, statusCode := invokeVCRestAPIGetRequest(vcRestSessionId, contentLibsFetchUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	v := []string{}
	gomega.Expect(json.Unmarshal(resp, &v)).NotTo(gomega.HaveOccurred())
	framework.Logf("Content lib IDs:\n%v", v)
	return v
}

// getContentLib fetches the content lib with give ID
func getContentLib(vcRestSessionId string, libId string, vs *vSphere) subscribedContentLibBasic {
	vcIp := e2eVSphere.Config.Global.VCenterHostname

	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		vcIp = GetStringEnvVarOrDefault("LOCAL_HOST_IP", defaultlocalhostIP)
	}

	contentLibFetchUrl := "https://" + vcIp + ":" + vs.Config.Global.VCenterPort +
		"/api/content/subscribed-library/" + libId

	resp, statusCode := invokeVCRestAPIGetRequest(vcRestSessionId, contentLibFetchUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	var v map[string]interface{}
	gomega.Expect(json.Unmarshal(resp, &v)).NotTo(gomega.HaveOccurred())

	var cl subscribedContentLibBasic
	cl.name = v["name"].(string)
	cl.Id = v["id"].(string)
	cl.ds_moId = v["storage_backings"].([]interface{})[0].(map[string]interface{})["datastore_id"].(string)
	cl.url = v["subscription_info"].(map[string]interface{})["subscription_url"].(string)

	framework.Logf("Content lib with id %v: %v", libId, cl)
	return cl
}

// invokeVCRestAPIGetRequest invokes GET on given VC REST URL using the passed session token and verifies that the
// return status code is 200
func invokeVCRestAPIGetRequest(vcRestSessionId string, url string) ([]byte, int) {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: transCfg}
	framework.Logf("Invoking GET on url: %s", url)
	req, err := http.NewRequest("GET", url, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add(vcRestSessionIdHeaderName, vcRestSessionId)

	resp, statusCode := httpRequest(httpClient, req)

	return resp, statusCode
}

// invokeVCRestAPIPostRequest invokes POST on given VC REST URL using the passed session token and request body
func invokeVCRestAPIPostRequest(vcRestSessionId string, url string, reqBody string) ([]byte, int) {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: transCfg}
	framework.Logf("Invoking POST on url: %s", url)
	req, err := http.NewRequest("POST", url, strings.NewReader(reqBody))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add(vcRestSessionIdHeaderName, vcRestSessionId)
	req.Header.Add("Content-type", "application/json")

	resp, statusCode := httpRequest(httpClient, req)

	return resp, statusCode
}

// invokeVCRestAPIDeleteRequest invokes DELETE on given VC REST URL using the passed session token
func invokeVCRestAPIDeleteRequest(vcRestSessionId string, url string) ([]byte, int) {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: transCfg}
	framework.Logf("Invoking DELETE on url: %s", url)
	req, err := http.NewRequest("DELETE", url, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Add(vcRestSessionIdHeaderName, vcRestSessionId)

	resp, statusCode := httpRequest(httpClient, req)

	return resp, statusCode
}

// waitNGetVmiForImageName waits and fetches VM image CR for given image name in the specified namespace
func waitNGetVmiForImageName(ctx context.Context, c ctlrclient.Client, imageName string) string {
	vmi := ""
	time.Sleep(pollTimeoutShort)
	err := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vmImagesList := &vmopv1.VirtualMachineImageList{}
			err := c.List(ctx, vmImagesList)
			defer ginkgo.GinkgoRecover()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, instance := range vmImagesList.Items {
				if instance.Status.ImageName == imageName {
					framework.Logf("Found vmi %v for image name %v", instance.Name, imageName)
					vmi = instance.Name
					return true, nil
				}
			}
			return false, nil
		})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return vmi
}

type CreateVmOptionsV3 struct {
	Namespace           string
	VmClass             string
	VMI                 string
	StorageClassName    string
	CloudInitSecretName string
	PVCs                []*v1.PersistentVolumeClaim
	CryptoSpec          *vmopv3.VirtualMachineCryptoSpec
	WaitForReadyStatus  bool
}

// createVmServiceVmV3 creates VM v3 via VM service with given options
func createVmServiceVmV3(ctx context.Context, c ctlrclient.Client, opts CreateVmOptionsV3) *vmopv3.VirtualMachine {
	gomega.Expect(opts.VMI).NotTo(gomega.BeEmpty())
	gomega.Expect(opts.StorageClassName).NotTo(gomega.BeEmpty())

	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	if opts.VmClass == "" {
		opts.VmClass = vmClassBestEffortSmall
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vols := []vmopv3.VirtualMachineVolume{}
	vmName := fmt.Sprintf("csi-test-vm-%d", r.Intn(10000))
	for _, pvc := range opts.PVCs {
		vols = append(vols, vmopv3.VirtualMachineVolume{
			Name: pvc.Name,
			VirtualMachineVolumeSource: vmopv3.VirtualMachineVolumeSource{
				PersistentVolumeClaim: &vmopv3.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			},
		})
	}

	vm := &vmopv3.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: opts.Namespace},
		Spec: vmopv3.VirtualMachineSpec{
			PowerState:   vmopv3.VirtualMachinePowerStateOn,
			ImageName:    opts.VMI,
			ClassName:    opts.VmClass,
			StorageClass: opts.StorageClassName,
			Volumes:      vols,
			Crypto:       opts.CryptoSpec,
		},
	}

	if opts.CloudInitSecretName != "" {
		vm.Spec.Bootstrap = &vmopv3.VirtualMachineBootstrapSpec{
			CloudInit: &vmopv3.VirtualMachineBootstrapCloudInitSpec{
				RawCloudConfig: &vmopv3common.SecretKeySelector{
					Name: opts.CloudInitSecretName,
					Key:  "user-data",
				},
			},
		}
	}

	err := c.Create(ctx, vm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	vmKey := ctlrclient.ObjectKey{Name: vmName, Namespace: opts.Namespace}

	err = wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			err := c.Get(ctx, vmKey, vm)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return false, nil
			}

			if opts.WaitForReadyStatus &&
				!slices.ContainsFunc(vm.GetConditions(), func(c metav1.Condition) bool {
					return c.Type == vmopv3.VirtualMachineReconcileReady && c.Status == metav1.ConditionTrue
				}) {
				return false, nil
			}

			return true, nil
		})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Found VM %s in namespace %s", vmName, opts.Namespace)

	return vm
}

// createVmServiceVmWithPvcs creates VM via VM service with given ns, sc, vmi, pvc(s) and bootstrap data for cloud init
func createVmServiceVmWithPvcs(ctx context.Context, c ctlrclient.Client, namespace string, vmClass string,
	pvcs []*v1.PersistentVolumeClaim, vmi string, storageClassName string, secretName string) *vmopv1.VirtualMachine {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vols := []vmopv1.VirtualMachineVolume{}
	vmName := fmt.Sprintf("csi-test-vm-%d", r.Intn(10000))

	for _, pvc := range pvcs {
		vols = append(vols, vmopv1.VirtualMachineVolume{
			Name: pvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
			},
		})
	}

	vm := vmopv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: namespace},
		Spec: vmopv1.VirtualMachineSpec{
			PowerState:   vmopv1.VirtualMachinePoweredOn,
			ImageName:    vmi,
			ClassName:    vmClass,
			StorageClass: storageClassName,
			Volumes:      vols,
			VmMetadata:   &vmopv1.VirtualMachineMetadata{Transport: cloudInitLabel, SecretName: secretName},
		},
	}
	err := c.Create(ctx, &vm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return waitNgetVmsvcVM(ctx, c, namespace, vmName)
}

// deleteVmServiceVm deletes VM via VM service
func deleteVmServiceVm(ctx context.Context, c ctlrclient.Client, namespace, name string) {
	err := c.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
	}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// getVmsvcVM fetches the vm from the specified ns
func getVmsvcVM(
	ctx context.Context, c ctlrclient.Client, namespace string, vmName string) (*vmopv1.VirtualMachine, error) {
	instanceKey := ctlrclient.ObjectKey{Name: vmName, Namespace: namespace}
	vm := &vmopv1.VirtualMachine{}
	err := c.Get(ctx, instanceKey, vm)
	return vm, err
}

// waitNgetVmsvcVM wait and fetch the vm CR from the specified ns
func waitNgetVmsvcVM(ctx context.Context, c ctlrclient.Client, namespace string, vmName string) *vmopv1.VirtualMachine {
	vm := &vmopv1.VirtualMachine{}
	var err error
	err = wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vm, err = getVmsvcVM(ctx, c, namespace, vmName)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return false, nil
			}
			return true, nil
		})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Found VM %s in namespace %s", vmName, namespace)
	return vm
}

// waitNgetVmsvcVmIp wait and fetch the primary IP of the vm in give ns
func waitNgetVmsvcVmIp(ctx context.Context, c ctlrclient.Client, namespace string, name string) (string, error) {
	ip := ""
	err := wait.PollUntilContextTimeout(ctx, poll*10, pollTimeout*4, true,
		func(ctx context.Context) (bool, error) {
			vm, err := getVmsvcVM(ctx, c, namespace, name)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return false, nil
			}
			if vm.Status.VmIp == "" {
				return false, nil
			}
			ip = vm.Status.VmIp
			return true, nil
		})
	framework.Logf("Found IP '%s' for VM '%s'", ip, name)
	return ip, err
}

// createBootstrapSecretForVmsvcVms create bootstrap data for cloud init in the ns
func createBootstrapSecretForVmsvcVms(ctx context.Context, client clientset.Interface, namespace string) string {
	secretSpec := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "vm-bootstrap-data", Namespace: namespace},
		StringData: map[string]string{"user-data": `#cloud-config
ssh_pwauth: true
users:
- default
- name: worker
  lock_passwd: false
  plain_text_passwd: 'ca$hc0w'
  sudo: ALL=(ALL) NOPASSWD:ALL
  shell: /bin/bash`},
	}
	secret, err := client.CoreV1().Secrets(namespace).Create(ctx, &secretSpec, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return secret.Name
}

// createService4Vm creates a virtualmachineservice(loadbalancer) for given vm in the specified ns
func createService4Vm(
	ctx context.Context, c ctlrclient.Client, namespace string, vmName string) *vmopv1.VirtualMachineService {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	svcName := fmt.Sprintf("%s-svc-%d", vmName, r.Intn(10000))
	framework.Logf("Creating loadbalancer VM: %s for vm: %s", svcName, vmName)
	vmService := vmopv1.VirtualMachineService{
		ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: namespace},
		Spec: vmopv1.VirtualMachineServiceSpec{
			Ports:    []vmopv1.VirtualMachineServicePort{{Name: "ssh", Port: 22, Protocol: "TCP", TargetPort: 22}},
			Type:     "LoadBalancer",
			Selector: map[string]string{"app": "vmName"},
		},
	}
	err := c.Create(ctx, &vmService)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return waitNgetVmLbSvc(ctx, c, namespace, svcName)
}

// getVmsvcVmLbSvc fetches the virtualmachineservice(loadbalancer) for given vm in the specified ns
func getVmsvcVmLbSvc(ctx context.Context, c ctlrclient.Client, namespace string, name string) (
	*vmopv1.VirtualMachineService, error) {
	instanceKey := ctlrclient.ObjectKey{Name: name, Namespace: namespace}
	svc := &vmopv1.VirtualMachineService{}
	err := c.Get(ctx, instanceKey, svc)
	return svc, err
}

// waitNgetVmLbSvc wait and fetches the virtualmachineservice(loadbalancer) for given vm in the specified ns
func waitNgetVmLbSvc(
	ctx context.Context, c ctlrclient.Client, namespace string, name string) *vmopv1.VirtualMachineService {
	vmLbSvc := &vmopv1.VirtualMachineService{}
	var err error
	err = wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vmLbSvc, err = getVmsvcVmLbSvc(ctx, c, namespace, name)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return false, nil
			}
			return true, nil
		})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return vmLbSvc
}

// verifyPvcsAreAttachedToVmsvcVm verify given pvc(s) is(are) attached to given VM via vm and cnsnodevmattachment CRs
func verifyPvcsAreAttachedToVmsvcVm(ctx context.Context, cnsc ctlrclient.Client,
	vm *vmopv1.VirtualMachine, pvcs []*v1.PersistentVolumeClaim) bool {

	attachmentmap := map[string]int{}
	pvcmap := map[string]int{}
	if len(vm.Status.Volumes) != len(pvcs) {
		framework.Logf("Found %d volumes in VM status vs %d pvcs sent to check for attachment",
			len(vm.Status.Volumes), len(pvcs))
		return false
	}

	for _, vol := range vm.Status.Volumes {
		pvcmap[vol.Name] = 1
		if vol.Attached {
			attachmentmap[vol.Name] = 1
		}
	}
	for _, pvc := range pvcs {
		if pvcmap[pvc.Name] == 1 {
			pvcmap[pvc.Name] = pvcmap[pvc.Name] + 1
		} else {
			pvcmap[pvc.Name] = 1
		}
		_, err := getCnsNodeVmAttachmentCR(ctx, cnsc, pvc.Namespace, vm.Name, pvc.Name)
		if err != nil {
			if !apierrors.IsNotFound(err) { // we will return false in attachmentmap check below for this case
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		} else {
			if attachmentmap[pvc.Name] == 1 {
				attachmentmap[pvc.Name] = attachmentmap[pvc.Name] + 1
			} else {
				attachmentmap[pvc.Name] = 1
			}
		}
	}
	match := true
	for entry := range pvcmap {
		if pvcmap[entry] != 2 {
			framework.Logf("PVC %s was not passed in the checklist or was not part of VM", entry)
			match = false
		}
	}
	for entry := range attachmentmap {
		if attachmentmap[entry] != 2 {
			framework.Logf("PVC %s was not attached to VM or did not have CnsNodeVmAttachment CR", entry)
			match = false
		}
	}
	framework.Logf("Given PVCs '%v' are attached to VM %s", reflect.ValueOf(pvcmap).MapKeys(), vm.Name)
	return match
}

// getCnsNodeVmAttachmentCR fetches the requested cnsnodevmattachment CRs
func getCnsNodeVmAttachmentCR(
	ctx context.Context, cnsc ctlrclient.Client, namespace string, vmName string, pvcName string) (
	*cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment, error) {

	instanceKey := ctlrclient.ObjectKey{Name: vmName + "-" + pvcName, Namespace: namespace}
	cr := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
	err := cnsc.Get(ctx, instanceKey, cr)
	return cr, err
}

// waitNverifyPvcsAreAttachedToVmsvcVm wait for pvc(s) th be attached to VM via vm and cnsnodevmattachment CRs
func waitNverifyPvcsAreAttachedToVmsvcVm(ctx context.Context, vmopC ctlrclient.Client, cnsopC ctlrclient.Client,
	vm *vmopv1.VirtualMachine, pvcs []*v1.PersistentVolumeClaim) error {

	err := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vm, err := getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
			if err != nil {
				return false, err
			}
			if verifyPvcsAreAttachedToVmsvcVm(ctx, cnsopC, vm, pvcs) {
				return true, nil
			}
			return false, nil
		})

	return err
}

// formatNVerifyPvcIsAccessible formats the PVC inside a VM, creates a filesystem,
// and returns a folder with 777 permissions under the mount point.
func formatNVerifyPvcIsAccessible(diskUuid string, mountIndex int, vmIp string) string {
	p := "/dev/disk/by-id/wwn-0x" + strings.ReplaceAll(strings.ToLower(diskUuid), "-", "")
	fmt.Println("Checking disk path:", p)

	var dev string
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		// Trigger SCSI rescan
		execSshOnVmThroughGatewayVm(vmIp, []string{
			"for host in /sys/class/scsi_host/host*; do echo '- - -' | sudo tee $host/scan; done",
		})

		// Resolve device path
		diskCheckResults := execSshOnVmThroughGatewayVm(vmIp, []string{
			fmt.Sprintf("ls -l %s || true", p),
			fmt.Sprintf("readlink -f %s || true", p),
		})
		dev = strings.TrimSpace(diskCheckResults[len(diskCheckResults)-1].Stdout)

		if dev == "" || dev == "/dev/" {
			fmt.Printf("Attempt %d/%d: device not resolved yet\n", i+1, maxRetries)
			time.Sleep(3 * time.Second)
			continue
		}

		// Skip root disk
		if dev == "/dev/sda" {
			fmt.Printf("Resolved device is root disk (/dev/sda). Skipping PVC format for UUID %s on VM %s\n", diskUuid, vmIp)

			// 🔎 Debug check: list all disks inside the VM
			checkResults := execSshOnVmThroughGatewayVm(vmIp, []string{
				"lsblk -o NAME,SIZE,TYPE,MOUNTPOINT",
				"sudo fdisk -l || true",
				"ls -l /dev/disk/by-id/ | grep wwn || true",
			})
			fmt.Println("Disk check results:")
			for _, r := range checkResults {
				fmt.Println(r.Stdout)
			}

			return "" // return safely instead of failing
		}

		break
	}

	if dev == "" || dev == "/dev/" {
		framework.Failf("Failed to resolve valid device path for UUID %s on VM %s (got %s)", p, vmIp, dev)
	}

	fmt.Println("Resolved device:", dev)
	partitionDev := dev + "1"

	// Unmount if already mounted
	execSshOnVmThroughGatewayVm(vmIp, []string{
		fmt.Sprintf("sudo umount -f %s || true", partitionDev),
		fmt.Sprintf("sudo umount -f %s || true", dev),
	})

	// Partition fresh (wipe GPT headers, then create ext4)
	partitionCommands := []string{
		fmt.Sprintf("sudo wipefs -a %s || true", dev),
		fmt.Sprintf("sudo parted --script %s mklabel gpt", dev),
		fmt.Sprintf("sudo parted --script -a optimal %s mkpart primary ext4 0%% 100%%", dev),
		"sleep 2",
		fmt.Sprintf("sudo mkfs.ext4 -F %s", partitionDev),
	}
	execSshOnVmThroughGatewayVm(vmIp, partitionCommands)

	// Mount point
	volMountPath := "/mnt/volume" + strconv.Itoa(mountIndex)
	volFolder := volMountPath + "/data"
	mountCommands := []string{
		fmt.Sprintf("sudo mkdir -p %s", volMountPath),
		fmt.Sprintf("sudo mount -o discard %s %s", partitionDev, volMountPath),
		fmt.Sprintf("sudo mkdir -p %s", volFolder),
		fmt.Sprintf("sudo chmod -R 777 %s", volFolder),
		fmt.Sprintf("df -Th %s | tee %s/fstype", partitionDev, volFolder),
		fmt.Sprintf("grep -c ext4 %s/fstype", volFolder),
	}
	results := execSshOnVmThroughGatewayVm(vmIp, mountCommands)

	// Verify FS type
	gomega.Expect(strings.TrimSpace(results[len(results)-1].Stdout)).To(gomega.Equal("1"), "Filesystem type is not ext4")

	return volFolder
}

// verifyDataIntegrityOnVmDisk verifies data integrity with 100m random data on given FS path inside a vm
func verifyDataIntegrityOnVmDisk(vmIp, volFolder string) {
	results := execSshOnVmThroughGatewayVm(vmIp, []string{"dd count=100 bs=1M if=/dev/urandom of=/tmp/file1",
		"dd count=100 bs=1M if=/tmp/file1 of=" + volFolder + "/vmfile",
		"dd count=100 bs=1M if=" + volFolder + "/vmfile of=/tmp/file2", "md5sum /tmp/file1 /tmp/file2",
	})
	lines := strings.Split(results[3].Stdout, "\n")
	gomega.Expect(strings.Fields(lines[0])[0]).To(gomega.Equal(strings.Fields(lines[1])[0]))
}

// execSshOnVmThroughGatewayVm executes cmd(s) on VM via gateway(bastion) host and returns the result(s)
func execSshOnVmThroughGatewayVm(vmIp string, cmds []string) []fssh.Result {
	results := []fssh.Result{}

	gatewayClient, sshClient := getSshClientForVmThroughGatewayVm(vmIp)
	defer sshClient.Close()
	defer gatewayClient.Close()

	for _, cmd := range cmds {
		sshSession, err := sshClient.NewSession()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		code := 0
		result := fssh.Result{Host: vmIp, Cmd: cmd}
		var bytesStdout, bytesStderr bytes.Buffer
		sshSession.Stdout, sshSession.Stderr = &bytesStdout, &bytesStderr
		if err = sshSession.Run(cmd); err != nil {
			if exiterr, ok := err.(*ssh.ExitError); ok {
				code = exiterr.ExitStatus()
			}
		}
		result.Stdout = bytesStdout.String()
		result.Stderr = bytesStderr.String()
		result.Code = code

		fssh.LogResult(result)
		sshSession.Close()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		results = append(results, result)
	}
	return results
}

// copyFileToVm copies a local file to a VM via gateway host
func copyFileToVm(vmIp string, localFilePath string, vmFilePath string) {
	gatewayClient, sshClient := getSshClientForVmThroughGatewayVm(vmIp)
	defer sshClient.Close()
	defer gatewayClient.Close()

	sftp, err := sftp.NewClient(sshClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer sftp.Close()

	// Open the source file
	localFile, err := os.Open(localFilePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer localFile.Close()

	// Create the destination file
	vmFile, err := sftp.Create(vmFilePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer vmFile.Close()

	// write to file
	n, err := vmFile.ReadFrom(localFile)
	framework.Logf("Read %d bytes", n)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// copyFileToVm copies a file from VM via gateway host
func copyFileFromVm(vmIp string, vmFilePath string, localFilePath string) {
	gatewayClient, sshClient := getSshClientForVmThroughGatewayVm(vmIp)
	defer sshClient.Close()
	defer gatewayClient.Close()

	sftp, err := sftp.NewClient(sshClient)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer sftp.Close()

	// Open the source file
	localFile, err := os.Create(localFilePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer localFile.Close()

	// Create the destination file
	vmFile, err := sftp.Open(vmFilePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer vmFile.Close()

	// write to file
	n, err := localFile.ReadFrom(vmFile)
	framework.Logf("Read %d bytes", n)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// getSshClientForVmThroughGatewayVm return a ssh client via gateway host for the given VM
func getSshClientForVmThroughGatewayVm(vmIp string) (*ssh.Client, *ssh.Client) {
	framework.Logf("gateway pwd: %s", GetAndExpectStringEnvVar(envGatewayVmPasswd))
	gatewayConfig := &ssh.ClientConfig{
		User: GetAndExpectStringEnvVar(envGatewayVmUser),
		Auth: []ssh.AuthMethod{
			ssh.Password(GetAndExpectStringEnvVar(envGatewayVmPasswd)),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	vmConfig := &ssh.ClientConfig{
		User: "worker",
		Auth: []ssh.AuthMethod{
			ssh.Password("ca$hc0w"),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	gatewayClient, err := ssh.Dial("tcp", GetAndExpectStringEnvVar(envGatewayVmIp)+":22", gatewayConfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("VM IP: %s", vmIp)
	conn, err := gatewayClient.Dial("tcp", vmIp+":22")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ncc, chans, reqs, err := ssh.NewClientConn(conn, vmIp, vmConfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return gatewayClient, ssh.NewClient(ncc, chans, reqs)
}

// wait4PvcAttachmentFailure waits for PVC attachment to given VM to fail
func wait4PvcAttachmentFailure(
	ctx context.Context, vmopC ctlrclient.Client, vm *vmopv1.VirtualMachine, pvc *v1.PersistentVolumeClaim) error {
	var returnErr error
	waitErr := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vm, err := getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, vol := range vm.Status.Volumes {
				if vol.Name == pvc.Name {
					if !vol.Attached {
						returnErr = fmt.Errorf("%+v", vol.Error)
						return true, nil
					}
					break
				}
			}
			return false, nil
		})
	gomega.Expect(waitErr).NotTo(gomega.HaveOccurred())
	return returnErr
}

// mountFormattedVol2Vm mounts a preformatted volume inside the VM
func mountFormattedVol2Vm(diskUuid string, mountIndex int, vmIp string) string {
	p := "/dev/disk/by-id/wwn-0x" + strings.ReplaceAll(strings.ToLower(diskUuid), "-", "")
	results := execSshOnVmThroughGatewayVm(vmIp, []string{"ls -l /dev/disk/by-id/", "ls -l " + p})
	dev := "/dev/" + strings.TrimSpace(strings.Split(results[1].Stdout, "/")[6])
	gomega.Expect(dev).ShouldNot(gomega.Equal("/dev/"))
	framework.Logf("Found %s dev for disk with uuid %s", dev, diskUuid)

	partitionDev := dev + "1"

	volMountPath := "/mnt/volume" + strconv.Itoa(mountIndex)
	volFolder := volMountPath + "/data"
	results = execSshOnVmThroughGatewayVm(vmIp, []string{
		"sudo mkdir -p " + volMountPath,
		"sudo mount " + partitionDev + " " + volMountPath,
		"sudo chmod -R 777 " + volFolder,
		"ls -lR " + volFolder,
		"grep -c ext4 " + volFolder + "/fstype",
	})
	gomega.Expect(strings.TrimSpace(results[4].Stdout)).To(gomega.Equal("1"))
	return volFolder
}

// setVmPowerState sets expected power state for the VM
func setVmPowerState(
	ctx context.Context, c ctlrclient.Client, vm *vmopv1.VirtualMachine,
	powerState vmopv1.VirtualMachinePowerState) *vmopv1.VirtualMachine {

	vm, err := getVmsvcVM(ctx, c, vm.Namespace, vm.Name) // refresh vm info
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vm.Spec.PowerState = powerState
	err = c.Update(ctx, vm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vm, err = getVmsvcVM(ctx, c, vm.Namespace, vm.Name) // refresh vm info
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return vm
}

// wait4Vm2ReachPowerStateInSpec wait for VM to reach expected power state
func wait4Vm2ReachPowerStateInSpec(
	ctx context.Context, c ctlrclient.Client, vm *vmopv1.VirtualMachine) (*vmopv1.VirtualMachine, error) {

	var err error
	waitErr := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vm, err = getVmsvcVM(ctx, c, vm.Namespace, vm.Name) // refresh vm info
			if err != nil {
				return false, err
			}
			if vm.Status.PowerState == vm.Spec.PowerState {
				return true, nil
			}
			return false, nil
		})
	framework.Logf("VM %s reached the power state %v requested in the spec", vm.Name, vm.Spec.PowerState)
	return vm, waitErr
}

// createVmServiceVmWithPvcsWithZone creates VM via VM service with given ns, sc, vmi, pvc(s) and bootstrap data for
// cloud init on given zone
func createVmServiceVmWithPvcsWithZone(ctx context.Context, c ctlrclient.Client, namespace string, vmClass string,
	pvcs []*v1.PersistentVolumeClaim, vmi string, storageClassName string, secretName string,
	zone string) *vmopv1.VirtualMachine {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vols := []vmopv1.VirtualMachineVolume{}
	vmName := fmt.Sprintf("csi-test-vm-%d", r.Intn(10000))
	for _, pvc := range pvcs {
		vols = append(vols, vmopv1.VirtualMachineVolume{
			Name: pvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
			},
		})
	}
	labels := make(map[string]string)
	labels["topology.kubernetes.io/zone"] = zone
	vm := vmopv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: namespace, Labels: labels},
		Spec: vmopv1.VirtualMachineSpec{
			PowerState:   vmopv1.VirtualMachinePoweredOn,
			ImageName:    vmi,
			ClassName:    vmClass,
			StorageClass: storageClassName,
			Volumes:      vols,
			VmMetadata:   &vmopv1.VirtualMachineMetadata{Transport: cloudInitLabel, SecretName: secretName},
		},
	}
	err := c.Create(ctx, &vm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return waitNgetVmsvcVM(ctx, c, namespace, vmName)
}

// wait4VmSvcVm2BeDeleted waits for the given vmservice vm to get deleted
func wait4VmSvcVm2BeDeleted(ctx context.Context, c ctlrclient.Client, vm *vmopv1.VirtualMachine) {
	waitErr := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			_, err := getVmsvcVM(ctx, c, vm.Namespace, vm.Name)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return true, nil
			}
			return false, nil
		})
	gomega.Expect(waitErr).NotTo(gomega.HaveOccurred())
}

// wait4Pvc2Detach waits for PVC to detach from given VM
func wait4Pvc2Detach(
	ctx context.Context, vmopC ctlrclient.Client, vm *vmopv1.VirtualMachine, pvc *v1.PersistentVolumeClaim) {
	waitErr := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vm, err := getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, vol := range vm.Status.Volumes {
				if vol.Name == pvc.Name {
					return false, nil
				}
			}
			return true, nil
		})
	gomega.Expect(waitErr).NotTo(gomega.HaveOccurred())
}

// createVMServiceVmWithMultiplePvcs creates a VMService VM
// and attaches this VM to a pvc and returns a list of created VMServiceVMs
func createVMServiceVmWithMultiplePvcs(ctx context.Context, c ctlrclient.Client, namespace string, vmClass string,
	pvcs []*v1.PersistentVolumeClaim, vmi string, storageClassName string, secretName string) []*vmopv1.VirtualMachine {
	var vms []*vmopv1.VirtualMachine
	for _, pvc := range pvcs {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		vols := []vmopv1.VirtualMachineVolume{}
		vmName := fmt.Sprintf("csi-test-vm-%d", r.Intn(10000))

		vols = append(vols, vmopv1.VirtualMachineVolume{
			Name: pvc.Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
			},
		})

		vm := vmopv1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: namespace},
			Spec: vmopv1.VirtualMachineSpec{
				PowerState:   vmopv1.VirtualMachinePoweredOn,
				ImageName:    vmi,
				ClassName:    vmClass,
				StorageClass: storageClassName,
				Volumes:      vols,
				VmMetadata:   &vmopv1.VirtualMachineMetadata{Transport: cloudInitLabel, SecretName: secretName},
			},
		}
		err := c.Create(ctx, &vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vms = append(vms, waitNgetVmsvcVM(ctx, c, namespace, vmName))
	}
	return vms
}

// createVMServiceVmInParallel creates VMService VM concurrently
// for a given namespace with 1:1 mapping between PVC and the VMServiceVM
func createVMServiceVmInParallel(ctx context.Context, c ctlrclient.Client, namespace string, vmClass string,
	pvcs []*v1.PersistentVolumeClaim, vmi string, storageClassName string, secretName string,
	vmCount int, ch chan *vmopv1.VirtualMachine, wg *sync.WaitGroup, lock *sync.Mutex) {
	defer wg.Done()
	for i := 0; i < vmCount; i++ {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		vols := []vmopv1.VirtualMachineVolume{}
		vmName := fmt.Sprintf("csi-test-vm-%d", r.Intn(10000))

		vols = append(vols, vmopv1.VirtualMachineVolume{
			Name: pvcs[i].Name,
			PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvcs[i].Name},
			},
		})

		vm := vmopv1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: namespace},
			Spec: vmopv1.VirtualMachineSpec{
				PowerState:   vmopv1.VirtualMachinePoweredOn,
				ImageName:    vmi,
				ClassName:    vmClass,
				StorageClass: storageClassName,
				Volumes:      vols,
				VmMetadata:   &vmopv1.VirtualMachineMetadata{Transport: cloudInitLabel, SecretName: secretName},
			},
		}
		err := c.Create(ctx, &vm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		lock.Lock()
		ch <- &vm
		lock.Unlock()
		framework.Logf("Created VMServiceVM: %s", vmName)
	}
}

// deleteVMServiceVmInParallel deletes the VMService VMs concurrently from a given namespace
func deleteVMServiceVmInParallel(ctx context.Context, c ctlrclient.Client,
	vms []*vmopv1.VirtualMachine, namespace string,
	wg *sync.WaitGroup) {

	defer wg.Done()
	for _, vm := range vms {
		err := c.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
			Name:      vm.Name,
			Namespace: namespace,
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// performVolumeLifecycleActionForVmServiceVM creates pvc and attaches a VMService VM to it
// and waits for the workloads to be in healthy state and then deletes them
func performVolumeLifecycleActionForVmServiceVM(ctx context.Context, client clientset.Interface,
	vmopC ctlrclient.Client, cnsopC ctlrclient.Client, vmClass string, namespace string, vmi string,
	sc *storagev1.StorageClass, secretName string) {
	ginkgo.By("Create a PVC")
	pvc, err := createPVC(ctx, client, namespace, nil, "", sc, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Waiting for all claims to be in bound state")
	pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv := pvs[0]
	volHandle := pv.Spec.CSI.VolumeHandle
	gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
	defer func() {
		ginkgo.By("Delete PVCs")
		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for CNS volumes to be deleted")
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}()

	ginkgo.By("Creating VM")
	vm := createVmServiceVmWithPvcs(
		ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, sc.Name, secretName)
	defer func() {
		ginkgo.By("Deleting VM")
		err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
			Name:      vm.Name,
			Namespace: namespace,
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Creating loadbalancing service for ssh with the VM")
	vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
	defer func() {
		ginkgo.By("Deleting loadbalancing service for ssh with the VM")
		err = vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
			Name:      vmlbsvc.Name,
			Namespace: namespace,
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Wait and verify PVCs are attached to the VM")
	gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
		[]*v1.PersistentVolumeClaim{pvc})).NotTo(gomega.HaveOccurred())
}

/*
updateVmWithNewPvc util updates vm volume attachment list by adding new
volumes to the vm
*/
func updateVmWithNewPvc(ctx context.Context, vmopC ctlrclient.Client, vmName string,
	namespace string, newPvc *v1.PersistentVolumeClaim) error {

	// Fetch the existing VM
	vm := &vmopv1.VirtualMachine{}
	err := vmopC.Get(ctx, ctlrclient.ObjectKey{Name: vmName, Namespace: namespace}, vm)
	if err != nil {
		return fmt.Errorf("failed to get VM: %v", err)
	}

	// Create a new volume using the new PVC
	newVolume := vmopv1.VirtualMachineVolume{
		Name: newPvc.Name,
		PersistentVolumeClaim: &vmopv1.PersistentVolumeClaimVolumeSource{
			PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
				ClaimName: newPvc.Name,
			},
		},
	}

	// Append the new volume to the existing VM's volumes
	vm.Spec.Volumes = append(vm.Spec.Volumes, newVolume)

	// Update the VM spec in the Kubernetes cluster
	if err = vmopC.Update(ctx, vm); err != nil {
		return fmt.Errorf("failed to update VM: %v", err)
	}
	return nil
}

// createVMServiceandWaitForVMtoGetIP creates a loadbalancing service for ssh with each VM
// and waits for VM IP to come up to come up and verify PVCs are accessible in the VM
func createVMServiceandWaitForVMtoGetIP(ctx context.Context, vmopC ctlrclient.Client,
	cnsopC ctlrclient.Client, namespace string, vms []*vmopv1.VirtualMachine,
	pvclaimsList []*v1.PersistentVolumeClaim, doCreateVmSvc bool, waitForVmIp bool) {

	if doCreateVmSvc {
		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		for _, vm := range vms {
			vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)
			defer func() {
				ginkgo.By("Deleting loadbalancing service for ssh with the VM")
				err := vmopC.Delete(ctx, &vmopv1.VirtualMachineService{ObjectMeta: metav1.ObjectMeta{
					Name:      vmlbsvc.Name,
					Namespace: namespace,
				}})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		}
	}

	if waitForVmIp {
		ginkgo.By("Wait for VM to come up and get an IP")
		for j, vm := range vms {
			vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Wait and verify PVCs are attached to the VM")
			gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
				[]*v1.PersistentVolumeClaim{pvclaimsList[j]})).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for i, vol := range vm.Status.Volumes {
				volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
				verifyDataIntegrityOnVmDisk(vmIp, volFolder)
			}
		}
	}
}

/*
This utility creates a VirtualMachine with specified PVCs, waits for the VM to be provisioned,
verifies PVC attachment, and ensures data integrity by verifying the accessibility of the disks
and verifying the attached volumes.
*/
func createVmServiceVm(ctx context.Context, client clientset.Interface, vmopC ctlrclient.Client,
	cnsopC ctlrclient.Client, namespace string,
	pvclaims []*v1.PersistentVolumeClaim, vmClass string,
	storageClassName string, createBootstrapSecret bool) (string, *vmopv1.VirtualMachine,
	*vmopv1.VirtualMachineService, error) {
	var err error
	var secretName string
	/*Fetch the VM image name from the environment variable. This image is used for
	creating the VirtualMachineInstance */
	vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
	framework.Logf("Waiting for virtual machine image list to be "+
		"available in namespace '%s' for image '%s'", namespace, vmImageName)
	vmi := waitNGetVmiForImageName(ctx, vmopC, vmImageName)

	/* Create a bootstrap secret for the VirtualMachineService VM. This secret contains
	credentials or configuration data needed by the VM. */
	if createBootstrapSecret {
		secretName = createBootstrapSecretForVmsvcVms(ctx, client, namespace)
	}

	var vm *vmopv1.VirtualMachine
	//Create the Virtual Machine with PVC
	if len(pvclaims) == 1 {
		vm = createVmServiceVmWithPvcs(ctx, vmopC, namespace, vmClass,
			[]*v1.PersistentVolumeClaim{pvclaims[0]}, vmi, storageClassName, secretName)
	} else {
		vm = createVmServiceVmWithPvcs(ctx, vmopC, namespace, vmClass,
			pvclaims, vmi, storageClassName, secretName)
	}

	// Create a service (load balancer) for the VM.
	vmlbsvc := createService4Vm(ctx, vmopC, namespace, vm.Name)

	// Wait for the VM to get an IP address.
	_, err = waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to get VM IP: %w", err)
	}

	// Verify that the PVCs are attached to the VirtualMachine.
	if len(pvclaims) == 1 {
		err = waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm, []*v1.PersistentVolumeClaim{pvclaims[0]})
	} else {
		err = waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm, pvclaims)
	}
	if err != nil {
		return "", nil, nil, fmt.Errorf("PVCs not attached to VM: %w", err)
	}

	vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info before returning
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return secretName, vm, vmlbsvc, nil
}

/*
This utility deletes a VirtualMachine, its associated load balancing service, and
the VM's bootstrap secret, ensuring a clean removal of all related resources.
*/
func deleteVmServiceVmWithItsConfig(ctx context.Context, client clientset.Interface, vmopC ctlrclient.Client,
	vmlbsvc *vmopv1.VirtualMachineService, namespace string, vm *vmopv1.VirtualMachine, secretName string) error {

	// Delete the load balancing service associated with the VM.
	if err := vmopC.Delete(ctx, &vmopv1.VirtualMachineService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmlbsvc.Name, // Name of the VirtualMachineService to delete.
			Namespace: namespace,    // Namespace where the service exists.
		},
	}); err != nil {
		return fmt.Errorf("failed to delete VirtualMachineService %q: %w", vmlbsvc.Name, err)
	}

	// Delete the Virtual Machine itself.
	if err := vmopC.Delete(ctx, &vmopv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vm.Name,   // Name of the VirtualMachine to delete.
			Namespace: namespace, // Namespace where the VirtualMachine exists.
		},
	}); err != nil {
		return fmt.Errorf("failed to delete VirtualMachine %q: %w", vm.Name, err)
	}

	//Delete the bootstrap secret for the VM.
	if err := client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0)); err != nil {
		return fmt.Errorf("failed to delete secret %q: %w", secretName, err)
	}
	return nil
}

/*
verifyAllowedTopologyLabelsForVmServiceVM checks if the VM has a
topology.kubernetes.io/zone label, verifies if its value is in the allowed zones
*/
func verifyAllowedTopologyLabelsForVmServiceVM(vm *vmopv1.VirtualMachine, allowedTopologies map[string][]string) error {
	label := vm.Labels

	// Check if the topologyKey label exists on the VM
	zone, labelExists := label[vmServiceVmLabelKey]
	if !labelExists {
		return fmt.Errorf("couldn't find label '%s' on svc pvc: %s", vmServiceVmLabelKey, vm.Name)
	}

	// Check if allowed zones for the key exist in allowedTopologies
	allowedZones, keyExists := allowedTopologies[vmServiceVmLabelKey]
	if !keyExists {
		return fmt.Errorf("couldn't find allowed topologies for key: %s", vmServiceVmLabelKey)
	}

	// Verify if the VM's zone is in the list of allowed zones
	if !containsItem(allowedZones, zone) {
		return fmt.Errorf("zone %q not found in allowed accessible "+
			"topologies: %v for svc pvc: %s", zone, allowedZones, vm.Name)
	}

	return nil
}

/*
Verifies if the virtual machine is running on a node that matches the
allowed topologies
*/
func verifyVmServiceVMNodeLocation(vm *vmopv1.VirtualMachine, nodeList *v1.NodeList,
	allowedTopologiesMap map[string][]string) (bool, error) {
	ip := strings.Replace(vm.Status.Host, ".", "-", -1)
	for _, node := range nodeList.Items {
		nodeName := strings.Replace(node.Name, ".", "-", -1)
		if strings.Contains(nodeName, ip) {
			for labelKey, labelValue := range node.Labels {
				if topologyValue, ok := allowedTopologiesMap[labelKey]; ok {
					if !isValuePresentInTheList(topologyValue, labelValue) {
						return false, fmt.Errorf("VM: %s is not running on node located in %s", vm.Name, labelValue)
					}
				}
			}
			return true, nil
		}
	}
	return false, fmt.Errorf("VM: %s is not running on any node with matching IP", vm.Name)
}

// getVmsvcVmDetailedOutput  gets the detailed status output of the vm
func getVmsvcVmDetailedOutput(ctx context.Context, c ctlrclient.Client, namespace string, name string) string {
	vm, _ := getVmsvcVM4(ctx, c, namespace, name)
	// Command to write data and sync it
	cmd := []string{"get", "vm", vm.Name, "-o", "yaml"}
	output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
	framework.Logf("Describe vm : %s", output)

	return output
}

// getVMStorageData returs the vmDiskUsage of the vm
func getVMStorageData(ctx context.Context, c ctlrclient.Client, namespace string, vmName string) string {
	yamlOutput := getVmsvcVmDetailedOutput(ctx, c, namespace, vmName)

	// Regex to match the line with "total: <value>"
	re := regexp.MustCompile(`(?i)total:\s*([^\s]+)`)
	matches := re.FindStringSubmatch(yamlOutput)
	framework.Logf("matches : %s", matches)
	if len(matches) < 2 {
		log.Fatal("Total value not found")
	}

	vmDiskUsage := matches[1]
	fmt.Println("Extracted vmDiskUsage:", vmDiskUsage)

	return vmDiskUsage
}

// getVmImages: get's all the images assigned to the given namespace
func getVmImages(ctx context.Context, namespace string) string {
	// Command to write data and sync it
	cmd := []string{"get", "vmi"}
	output := e2ekubectl.RunKubectlOrDie(namespace, cmd...)
	framework.Logf("StatusCode of addContentLibToNamespace : %s", output)

	return output
}

// Waits for vm images to get listed in namespace
func pollWaitForVMImageToSync(ctx context.Context, namespace string, expectedImage string, Poll,
	timeout time.Duration) error {

	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		listOfVmImages := getVmImages(ctx, namespace)
		// Split output into lines and search for the expected image
		lines := strings.Split(listOfVmImages, "\n")
		found := false
		for _, line := range lines {
			if strings.Contains(line, expectedImage) {
				found = true
				framework.Logf("Found : %t, Image: %s\n", found, expectedImage)
				break
			}
		}
		if !found {
			continue
		} else {
			return nil
		}

	}
	return fmt.Errorf("failed to load vm-image timed out after %v", timeout)

}

// get zone name on which vm is scheduled
func getVMzone(ctx context.Context, vm *vmopv4.VirtualMachine) (string, error) {
	vmlabel := vm.GetLabels()
	val, labelOk := vmlabel[vmZoneLabel]
	framework.Logf("val %v, labelOk: %v", val, labelOk)
	if !labelOk {
		return val, fmt.Errorf("zone is not present on vm: %s", vm.Name)
	}
	// Get labels and print them
	framework.Logf("vm Labels")
	vmlabel = vm.GetLabels()
	for k, v := range vmlabel {
		fmt.Printf("%s = %s\n", k, v)
	}

	return val, nil
}

type CreateVmOptionsV4 struct {
	Namespace          string
	VmClass            string
	VMI                string
	StorageClassName   string
	PVCs               []*v1.PersistentVolumeClaim
	SecretName         string
	WaitForReadyStatus bool
}

// createVmServiceVmV4 creates VM v3 via VM service with given options
func createVmServiceVmV4(ctx context.Context, c ctlrclient.Client, opts CreateVmOptionsV4) *vmopv4.VirtualMachine {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vols := []vmopv4.VirtualMachineVolume{}
	vmName := fmt.Sprintf("csi-test-vm-%d", r.Intn(10000))

	if opts.VmClass == "" {
		opts.VmClass = vmClassBestEffortSmall
	}

	for _, pvc := range opts.PVCs {
		vols = append(vols, vmopv4.VirtualMachineVolume{
			Name: pvc.Name,
			VirtualMachineVolumeSource: vmopv4.VirtualMachineVolumeSource{
				PersistentVolumeClaim: &vmopv4.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				},
			},
		})
	}

	vm := &vmopv4.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: opts.Namespace},
		Spec: vmopv4.VirtualMachineSpec{
			PowerState:   vmopv4.VirtualMachinePowerStateOn,
			ImageName:    opts.VMI,
			ClassName:    opts.VmClass,
			StorageClass: opts.StorageClassName,
			Volumes:      vols,
		},
	}

	if opts.SecretName != "" {
		vm.Spec.Bootstrap = &vmopv4.VirtualMachineBootstrapSpec{
			CloudInit: &vmopv4.VirtualMachineBootstrapCloudInitSpec{
				RawCloudConfig: &vmopv4common.SecretKeySelector{
					Name: opts.SecretName,
					Key:  opts.SecretName,
				},
			},
		}
	}

	err := c.Create(ctx, vm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	vmKey := ctlrclient.ObjectKey{Name: vmName, Namespace: opts.Namespace}

	err = wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			err := c.Get(ctx, vmKey, vm)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return false, nil
			}

			if opts.WaitForReadyStatus &&
				!slices.ContainsFunc(vm.GetConditions(), func(c metav1.Condition) bool {
					return c.Type == vmopv4.VirtualMachineReconcileReady && c.Status == metav1.ConditionTrue
				}) {
				return false, nil
			}

			return true, nil
		})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Found VM %s in namespace %s", vmName, opts.Namespace)

	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	svcName := fmt.Sprintf("%s-svc-%d", vmName, r.Intn(10000))
	framework.Logf("Creating loadbalancer VM: %s for vm: %s", svcName, vmName)
	vmService := vmopv4.VirtualMachineService{
		ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: opts.Namespace},
		Spec: vmopv4.VirtualMachineServiceSpec{
			Ports:    []vmopv4.VirtualMachineServicePort{{Name: "ssh", Port: 22, Protocol: "TCP", TargetPort: 22}},
			Type:     "LoadBalancer",
			Selector: map[string]string{"app": "vmName"},
		},
	}
	err = c.Create(ctx, &vmService)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	vm, _ = getVmsvcVmV4(ctx, c, opts.Namespace, vmName)
	framework.Logf("Found VM %s in namespace %s", vmName, opts.Namespace)

	return vm
}

// getVmsvcVM fetches the vm from the specified ns
func getVmsvcVmV4(
	ctx context.Context, c ctlrclient.Client, namespace string, vmName string) (*vmopv4.VirtualMachine, error) {
	instanceKey := ctlrclient.ObjectKey{Name: vmName, Namespace: namespace}
	vm := &vmopv4.VirtualMachine{}
	err := c.Get(ctx, instanceKey, vm)
	return vm, err
}

// getVmsvcVM fetches the vm from the specified ns
func getVmsvcVM4(
	ctx context.Context, c ctlrclient.Client, namespace string, vmName string) (*vmopv4.VirtualMachine, error) {
	instanceKey := ctlrclient.ObjectKey{Name: vmName, Namespace: namespace}
	vm := &vmopv4.VirtualMachine{}
	err := c.Get(ctx, instanceKey, vm)
	return vm, err
}

// getVmsvcVM fetches the vm from the specified ns
func getVmsvcVM5(
	ctx context.Context, c ctlrclient.Client, namespace string, vmName string) (*vmopv5.VirtualMachine, error) {
	instanceKey := ctlrclient.ObjectKey{Name: vmName, Namespace: namespace}
	vm := &vmopv5.VirtualMachine{}
	err := c.Get(ctx, instanceKey, vm)
	return vm, err
}

// waitNgetVmsvcVmIp wait and fetch the primary IP of the vm in give ns
func waitNgetVmsvcVmIpV4(ctx context.Context, c ctlrclient.Client, namespace string, name string) (string, error) {
	ip := ""
	err := wait.PollUntilContextTimeout(ctx, poll*10, pollTimeout*4, true,
		func(ctx context.Context) (bool, error) {
			vm, err := getVmsvcVmV4(ctx, c, namespace, name)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return false, nil
			}
			if vm.Status.Network.PrimaryIP4 == "" {
				return false, nil
			}
			ip = vm.Status.Network.PrimaryIP4
			return true, nil
		})
	framework.Logf("Found IP '%s' for VM '%s'", ip, name)
	return ip, err
}
