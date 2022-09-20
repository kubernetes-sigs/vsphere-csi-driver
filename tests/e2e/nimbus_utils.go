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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/davecgh/go-spew/spew"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
)

type TestbedBasicInfo struct {
	name     string `default:"worker"`
	user     string
	location string
	vcIp     string
	vcVmName string
	esxHosts []map[string]string
	podname  string
}

var tbinfo TestbedBasicInfo

// vMPowerMgmt power on/off given nimbus VMs (space separated list)
func vMPowerMgmt(user string, location string, podname string, hostList string, shouldBePoweredOn bool) error {
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

// readVcEsxIpsViaTestbedInfoJson read basic testbed info from the json file
func readVcEsxIpsViaTestbedInfoJson(filePath string) {
	tbinfo = TestbedBasicInfo{}

	file, err := os.ReadFile(filePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	framework.Logf("Fetching basic testbed info from json file")

	var tb map[string]interface{}
	err = json.Unmarshal(file, &tb)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	vcs := tb["vc"].([]interface{})
	vc1 := vcs[0].(map[string]interface{})
	tbinfo.vcIp = vc1["ip"].(string)
	tbinfo.vcVmName = vc1["name"].(string)

	esxs := tb["esx"].([]interface{})

	esxHosts := []map[string]string{}

	for _, esx := range esxs {
		host := make(map[string]string)
		host["ip"] = esx.(map[string]interface{})["ip"].(string)
		host["vmName"] = esx.(map[string]interface{})["name"].(string)
		esxHosts = append(esxHosts, host)
	}
	tbinfo.esxHosts = esxHosts
	tbinfo.name = tb["name"].(string)
	tbinfo.user = tb["user_name"].(string)
	tbinfo.location = tb["nimbusLocation"].(string)
	tbinfo.podname = tb["podname"].(string)

	framework.Logf("Basic testbed info:\n%s\n", spew.Sdump(tbinfo))
}
