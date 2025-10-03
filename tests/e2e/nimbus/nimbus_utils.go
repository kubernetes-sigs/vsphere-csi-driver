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

package nimbus

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
)

type TestbedBasicInfo struct {
	Name       string `default:"worker"`
	User       string
	Location   string
	VcIp       string
	VcVmName   string
	EsxHosts   []map[string]string
	Podname    string
	Datastores []map[string]string
}

var Tbinfo TestbedBasicInfo

// This function provides power on/off functionality to nimbus VMs (space separated list)
func VMPowerMgmt(user string, location string, podname string, hostList string, shouldBePoweredOn bool) error {
	var err error
	op := "off"
	if shouldBePoweredOn {
		op = "on"
	}
	nimbusCmd := fmt.Sprintf("USER=%s /mts/git/bin/nimbus-ctl --nimbusLocation %s --nimbus %s %s %s", user,
		location, podname, op, hostList)
	framework.Logf("Running command: %s", nimbusCmd)
	cmd := exec.Command("/bin/bash", "-c", nimbusCmd)
	err = cmd.Start()
	if err != nil {
		return err
	}
	err = cmd.Wait()

	framework.Logf("stdout:\n%v\nstderr:\n%v\n", cmd.Stdout, cmd.Stderr)
	return err
}

// This function is used to perform datatsore nimbus operations
func DatatoreOperations(user string, location string, podname string, vmName string, op string) error {
	var err error
	nimbusCmd := fmt.Sprintf("USER=%s /mts/git/bin/nimbus-ctl --nimbusLocation %s --nimbus %s %s %s", user,
		location, podname, op, vmName)
	framework.Logf("Running command: %s", nimbusCmd)
	cmd := exec.Command("/bin/bash", "-c", nimbusCmd)
	err = cmd.Start()
	if err != nil {
		return err
	}
	err = cmd.Wait()

	framework.Logf("stdout:\n%v\nstderr:\n%v\n", cmd.Stdout, cmd.Stderr)
	return err
}

// This function read basic testbed info from the provided json file
func ReadVcEsxIpsViaTestbedInfoJson(filePath string) {
	file, err := os.ReadFile(filePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Fetching basic testbed info from json file")

	var tb map[string]interface{}
	err = json.Unmarshal(file, &tb)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	vcs := tb["vc"].([]interface{})
	vc1 := vcs[0].(map[string]interface{})
	Tbinfo.VcIp = vc1["ip"].(string)
	Tbinfo.VcVmName = vc1["name"].(string)

	esxs := tb["esx"].([]interface{})
	nfsDS := tb["nfs"].([]interface{})
	iscsiDS := tb["iscsi"].([]interface{})

	esxHosts := []map[string]string{}
	nfsDatastores := []map[string]string{}
	iscsiDatastores := []map[string]string{}

	for _, esx := range esxs {
		host := make(map[string]string)
		host["ip"] = esx.(map[string]interface{})["ip"].(string)
		host["vmName"] = esx.(map[string]interface{})["name"].(string)
		esxHosts = append(esxHosts, host)
	}

	for _, nfs := range nfsDS {
		ds := make(map[string]string)
		ds["ip"] = nfs.(map[string]interface{})["ip"].(string)
		ds["vmName"] = nfs.(map[string]interface{})["name"].(string)
		nfsDatastores = append(nfsDatastores, ds)
	}

	for _, iscsi := range iscsiDS {
		ds := make(map[string]string)
		ds["ip"] = iscsi.(map[string]interface{})["ip"].(string)
		ds["vmName"] = iscsi.(map[string]interface{})["name"].(string)
		iscsiDatastores = append(iscsiDatastores, ds)
	}
	iscsiDatastores = append(iscsiDatastores, nfsDatastores...)

	Tbinfo.EsxHosts = esxHosts
	Tbinfo.Datastores = iscsiDatastores

	Tbinfo.Name = tb["name"].(string)
	Tbinfo.User = tb["user_name"].(string)
	Tbinfo.Location = tb["nimbusLocation"].(string)
	Tbinfo.Podname = tb["podname"].(string)

	framework.Logf("Basic testbed info:\n%s\n", spew.Sdump(Tbinfo))
}
