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

package e2e

import (
	"context"
	"fmt"
	"sync"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/test/e2e/framework"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

// setVpxdTaskTimeout sets vpxd task timeout to given number of seconds
// Following cases will be handled here
//  1. Timeout is not set, and we want to set it
//  2. Timeout is set we want to clear it
//  3. different timeout is set, we want to change it
//  4. timeout is not set/set to a number, and that is what we want
//
// default task timeout is 40 mins
// if taskTimeout param is 0 we will remove the timeout entry in cfg file and default timeout will kick-in
func setVpxdTaskTimeout(ctx context.Context, taskTimeout int) {
	var err error
	timeoutMatches := false
	diffTimeoutExists := false

	// Read hosts sshd port number
	ip, portNum, err := getPortNumAndIP(vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := ip + ":" + portNum

	grepCmd := "grep '<timeout>' /etc/vmware-vpx/vpxd.cfg"
	framework.Logf("Invoking command '%v' on vCenter host %v", grepCmd, ip)
	result, err := fssh.SSH(ctx, grepCmd, addr, framework.TestContext.Provider)
	if err != nil {
		fssh.LogResult(result)
		err = fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", grepCmd, addr, err)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// cmd to add the timeout value to the file
	sshCmd := fmt.Sprintf(
		"sed -i 's/<task>/<task>\\n    <timeout>%v<\\/timeout>/' /etc/vmware-vpx/vpxd.cfg", taskTimeout)
	if result.Code == 0 {
		grepCmd2 := fmt.Sprintf("grep '<timeout>%v</timeout>' /etc/vmware-vpx/vpxd.cfg", taskTimeout)
		framework.Logf("Invoking command '%v' on vCenter host %v", grepCmd2, addr)
		result2, err := fssh.SSH(ctx, grepCmd2, addr, framework.TestContext.Provider)
		if err != nil {
			fssh.LogResult(result)
			err = fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", grepCmd2, addr, err)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if result2.Code == 0 {
			timeoutMatches = true
		} else {
			diffTimeoutExists = true
		}
	} else {
		if taskTimeout == 0 {
			timeoutMatches = true
		}
	}
	if timeoutMatches {
		framework.Logf("vpxd timeout already matches, nothing to do ...")
		return
	}
	if diffTimeoutExists {
		sshCmd = fmt.Sprintf(
			"sed -i 's/<timeout>[0-9]*<\\/timeout>/<timeout>%v<\\/timeout>/' /etc/vmware-vpx/vpxd.cfg", taskTimeout)
	}
	if taskTimeout == 0 {
		sshCmd = "sed -i '/<timeout>[0-9]*<\\/timeout>/d' /etc/vmware-vpx/vpxd.cfg"
	}

	framework.Logf("Invoking command '%v' on vCenter host %v", sshCmd, addr)
	result, err = fssh.SSH(ctx, sshCmd, addr, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		fssh.LogResult(result)
		err = fmt.Errorf("couldn't execute command: %s on vCenter host %v: %v", sshCmd, addr, err)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// restart vpxd after changing the timeout
	err = invokeVCenterServiceControl(ctx, restartOperation, vpxdServiceName, vcAddress)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = waitVCenterServiceToBeInState(ctx, vpxdServiceName, vcAddress, svcRunningMessage)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	connect(ctx, &e2eVSphere)

	govmomiClient := newClient(ctx, &e2eVSphere)
	pc = newPbmClient(ctx, govmomiClient)
}

// stopHostD is a function for waitGroup to run stop hostd parallelly
func stopHostD(ctx context.Context, addr string, wg *sync.WaitGroup) {
	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	stopHostDOnHost(ctx, addr)
}
