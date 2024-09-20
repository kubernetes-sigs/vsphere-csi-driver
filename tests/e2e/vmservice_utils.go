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
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/gomega"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
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

// createTestWcpNs create a wcp namespace with given storage policy, vm class and content lib via REST API
func createTestWcpNs(
	vcRestSessionId string, storagePolicyId string, vmClass string, contentLibId string,
	supervisorId string) string {

	vcIp := e2eVSphere.Config.Global.VCenterHostname
	r := rand.New(rand.NewSource(time.Now().Unix()))

	namespace := fmt.Sprintf("csi-vmsvcns-%v", r.Intn(10000))
	nsCreationUrl := "https://" + vcIp + "/api/vcenter/namespaces/instances/v2"
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

	_, statusCode := invokeVCRestAPIPostRequest(vcRestSessionId, nsCreationUrl, reqBody)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 204))
	framework.Logf("Successfully created namepsace %v in SVC.", namespace)
	return namespace
}

// delTestWcpNs triggeres a wcp namespace deletion asynchronously
func delTestWcpNs(vcRestSessionId string, namespace string) {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	nsDeletionUrl := "https://" + vcIp + "/api/vcenter/namespaces/instances/" + namespace
	_, statusCode := invokeVCRestAPIDeleteRequest(vcRestSessionId, nsDeletionUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 204))
	framework.Logf("Successfully Deleted namepsace %v in SVC.", namespace)
}

// getSvcId fetches the ID of the Supervisor cluster
func getSvcId(vcRestSessionId string) string {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	svcIdFetchUrl := "https://" + vcIp + "/api/vcenter/namespace-management/supervisors/summaries"

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
	sslThumbPrint string) string {

	r := rand.New(rand.NewSource(time.Now().Unix()))
	contentlibName := fmt.Sprintf("csi-vmsvc-%v", r.Intn(10000))
	contentLibId, err := getContentLibId4Url(vcRestSessionId, contentLibUrl)
	if err == nil {
		gomega.Expect(contentLibId).NotTo(gomega.BeEmpty())
		return contentLibId
	}

	vcIp := e2eVSphere.Config.Global.VCenterHostname
	contentlbCreationUrl := "https://" + vcIp + "/api/content/subscribed-library"
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
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 201))

	gomega.Expect(json.Unmarshal(resp, &contentLibId)).NotTo(gomega.HaveOccurred())
	gomega.Expect(contentLibId).NotTo(gomega.BeEmpty())
	framework.Logf("Successfully created content library %s for the test(id: %v)", contentlibName, contentLibId)

	return contentLibId
}

// getContentLibId4Url fetches ID of a content lib that matches the given URL
func getContentLibId4Url(vcRestSessionId string, url string) (string, error) {
	var libId string
	libIds := getAllContentLibIds(vcRestSessionId)
	for _, libId := range libIds {
		lib := getContentLib(vcRestSessionId, libId)
		if lib.url == url {
			return libId, nil
		}
	}
	return libId, fmt.Errorf("couldn't find a content lib with subscription url '%v'", url)
}

// getAllContentLibIds fetches IDs of all content libs
func getAllContentLibIds(vcRestSessionId string) []string {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	contentLibsFetchUrl := "https://" + vcIp + "/api/content/subscribed-library"

	resp, statusCode := invokeVCRestAPIGetRequest(vcRestSessionId, contentLibsFetchUrl)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 200))

	v := []string{}
	gomega.Expect(json.Unmarshal(resp, &v)).NotTo(gomega.HaveOccurred())
	framework.Logf("Content lib IDs:\n%v", v)
	return v
}

// getContentLib fetches the content lib with give ID
func getContentLib(vcRestSessionId string, libId string) subscribedContentLibBasic {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	contentLibFetchUrl := "https://" + vcIp + "/api/content/subscribed-library/" + libId

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
func waitNGetVmiForImageName(ctx context.Context, c ctlrclient.Client, namespace string, imageName string) string {
	vmi := ""
	err := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			vmImagesList := &vmopv1.VirtualMachineImageList{}
			err := c.List(ctx, vmImagesList)
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
			VmMetadata:   &vmopv1.VirtualMachineMetadata{Transport: "CloudInit", SecretName: secretName},
		},
	}
	err := c.Create(ctx, &vm)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return waitNgetVmsvcVM(ctx, c, namespace, vmName)
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
	err := wait.PollUntilContextTimeout(ctx, poll*10, pollTimeout*6, true,
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
	fmt.Println(secretSpec)
	return secret.Name
}

// createService4Vm creates a virtualmachineservice(loadbalancer) for given vm in the specified ns
func createService4Vm(
	ctx context.Context, c ctlrclient.Client, namespace string, vmName string) *vmopv1.VirtualMachineService {
	svcName := vmName + "-svc"
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

// formatNVerifyPvcIsAccessible format the pvc inside vm and create a file system on it and returns a folder with 777
// permissions under the mount point
func formatNVerifyPvcIsAccessible(diskUuid string, mountIndex int, vmIp string) string {
	p := "/dev/disk/by-id/wwn-0x" + strings.ReplaceAll(strings.ToLower(diskUuid), "-", "")
	fmt.Println(p)
	results := execSshOnVmThroughGatewayVm(vmIp, []string{"ls -l /dev/disk/by-id/", "ls -l " + p})
	fmt.Println(results)
	dev := "/dev/" + strings.TrimSpace(strings.Split(results[1].Stdout, "/")[6])
	fmt.Println(dev)
	gomega.Expect(dev).ShouldNot(gomega.Equal("/dev/"))
	framework.Logf("Found %s dev for disk with uuid %s", dev, diskUuid)

	partitionDev := dev + "1"
	fmt.Println(partitionDev)
	res := execSshOnVmThroughGatewayVm(vmIp, []string{"sudo parted --script " + dev + " mklabel gpt",
		"sudo parted --script -a optimal " + dev + " mkpart primary 0% 100%", "lsblk -l",
		"sudo mkfs.ext4 " + partitionDev})
	fmt.Println(res)

	volMountPath := "/mnt/volume" + strconv.Itoa(mountIndex)
	fmt.Println(volMountPath)
	volFolder := volMountPath + "/data"
	fmt.Println(volFolder)
	results = execSshOnVmThroughGatewayVm(vmIp, []string{
		"sudo mkdir -p " + volMountPath,
		"sudo mount " + partitionDev + " " + volMountPath,
		"sudo mkdir -p " + volFolder,
		"sudo chmod -R 777 " + volFolder,
		fmt.Sprintf("bash -c 'df -Th %s | tee %s/fstype'", partitionDev, volFolder),
		"grep -c ext4 " + volFolder + "/fstype",
		"sync",
	})
	fmt.Println(results)
	gomega.Expect(strings.TrimSpace(results[5].Stdout)).To(gomega.Equal("1"))
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
	fmt.Println(vmIp)
	fmt.Println(cmds)
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
						returnErr = fmt.Errorf(vol.Error)
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
			VmMetadata:   &vmopv1.VirtualMachineMetadata{Transport: "CloudInit", SecretName: secretName},
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

func updateVmWithNewPvc(ctx context.Context, vmopC ctlrclient.Client, vmName string, namespace string, newPvc *v1.PersistentVolumeClaim) error {
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

func formatNVerifyPvcIsAccessible1(diskUuid string, mountIndex int, vmIp string) string {
    // Construct the disk path from the UUID
    p := "/dev/disk/by-id/wwn-0x" + strings.ReplaceAll(strings.ToLower(diskUuid), "-", "")
    fmt.Println("Checking disk path:", p)

    // List the available disks
    results := execSshOnVmThroughGatewayVm(vmIp, []string{
        "ls -l /dev/disk/by-id/",
    })
    fmt.Println("Disk list results:", results)

    // Check if the desired disk exists
    diskCheckResults := execSshOnVmThroughGatewayVm(vmIp, []string{
        "ls -l " + p,
    })

    // If the disk is not found, try rescanning SCSI devices
    if strings.Contains(diskCheckResults[0].Stderr, "No such file or directory") {
        fmt.Printf("Disk %s not found. Rescanning SCSI devices.\n", p)
        rescanResults := execSshOnVmThroughGatewayVm(vmIp, []string{
            "echo '- - -' | sudo tee /sys/class/scsi_host/host*/scan",
            "ls -l /dev/disk/by-id/",
            "ls -l " + p,
        })
        fmt.Println("Rescan results:", rescanResults)

        // Check again if the disk is available after rescanning
        diskCheckResults = execSshOnVmThroughGatewayVm(vmIp, []string{
            "ls -l " + p,
        })
    }

    // If the disk is still not found, fail the test
    if strings.Contains(diskCheckResults[0].Stderr, "No such file or directory") {
        framework.Failf("Disk %s not found on VM %s after rescanning.", p, vmIp)
    }

    // Extract the device name
    parts := strings.Split(strings.TrimSpace(diskCheckResults[0].Stdout), "/")
    if len(parts) < 7 {
        framework.Failf("Unexpected ls output: %s", diskCheckResults[0].Stdout)
    }
    dev := "/dev/" + parts[6]
    fmt.Println("Device:", dev)

    gomega.Expect(dev).ShouldNot(gomega.Equal("/dev/"))
    framework.Logf("Found device %s for disk with UUID %s", dev, diskUuid)

    partitionDev := dev + "1"
    fmt.Println("Partition Device:", partitionDev)

    // Unmount any existing partitions on the device
    unmountCommands := []string{
        fmt.Sprintf("sudo umount %s* || true", dev),
    }
    res := execSshOnVmThroughGatewayVm(vmIp, unmountCommands)
    fmt.Println("Unmount Results:", res)

    // Partition and format the disk
    partitionCommands := []string{
        fmt.Sprintf("sudo parted --script %s mklabel gpt", dev),
        fmt.Sprintf("sudo parted --script -a optimal %s mkpart primary 0%% 100%%", dev),
        "lsblk -l",
        fmt.Sprintf("sudo mkfs.ext4 %s", partitionDev),
    }
    res = execSshOnVmThroughGatewayVm(vmIp, partitionCommands)
    fmt.Println("Partitioning Results:", res)

    // Mount the new partition
    volMountPath := "/mnt/volume" + strconv.Itoa(mountIndex)
    volFolder := volMountPath + "/data"
    mountCommands := []string{
        fmt.Sprintf("sudo mkdir -p %s", volMountPath),
        fmt.Sprintf("sudo mount %s %s", partitionDev, volMountPath),
        fmt.Sprintf("sudo mkdir -p %s", volFolder),
        fmt.Sprintf("sudo chmod -R 777 %s", volFolder),
        fmt.Sprintf("bash -c 'df -Th %s | tee %s/fstype'", partitionDev, volFolder),
        fmt.Sprintf("grep -c ext4 %s/fstype", volFolder),
        "sync",
    }
    results = execSshOnVmThroughGatewayVm(vmIp, mountCommands)
    fmt.Println("Mounting Results:", results)

    // Verify the filesystem type
    gomega.Expect(strings.TrimSpace(results[5].Stdout)).To(gomega.Equal("1"), "Filesystem type is not ext4")

    return volFolder
}


