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
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
)

// writeRandomDataOnPod runs dd on the given pod and write count in Mib
func writeRandomDataOnPod(pod *v1.Pod, count int64) {
	var cmd []string
	if windowsEnv {
		cmd = []string{
			"exec",
			pod.Name,
			"--namespace=" + pod.Namespace,
			"powershell.exe",
			"$out = New-Object byte[] 536870912; (New-Object Random).NextBytes($out); " +
				"[System.IO.File]::WriteAllBytes('/mnt/volume1/testdata2.txt', $out)",
		}
	} else {
		cmd = []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
			"/bin/sh", "-c", "dd if=/dev/urandom of=/mnt/volume1/f1 bs=1M count=" + strconv.FormatInt(count, 10)}
	}
	_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, cmd...)
}

// writeKnownData2PodInParallel writes known 1mb data to a file in given pod's volume until 200mb is left in the volume
// in parallel
func writeKnownData2PodInParallel(
	f *framework.Framework, client clientset.Interface, pod *v1.Pod, testdataFile string, wg *sync.WaitGroup,
	size ...int64) {

	defer ginkgo.GinkgoRecover()
	defer wg.Done()
	writeKnownData2Pod(f, client, pod, testdataFile, size...)
}

// writeKnownData2Pod writes known 1mb data to a file in given pod's volume until 200mb is left in the volume
func writeKnownData2Pod(f *framework.Framework, client clientset.Interface, pod *v1.Pod, testdataFile string,
	size ...int64) {
	var svcMasterIp string
	var sshWcpConfig *ssh.ClientConfig
	if wcpVsanDirectCluster {
		svcMasterIp = GetAndExpectStringEnvVar(svcMasterIP)
		svcMasterPwd := GetAndExpectStringEnvVar(svcMasterPassword)
		framework.Logf("svc master ip: %s", svcMasterIp)
		sshWcpConfig = &ssh.ClientConfig{
			User: rootUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, "cp", testdataFile, fmt.Sprintf(
			"%v/%v:data0/testdata", pod.Namespace, pod.Name))
	} else {
		if windowsEnv {
			cmdTestData := []string{
				"exec",
				pod.Name,
				"--namespace=" + pod.Namespace,
				"powershell.exe",
				"$out = New-Object byte[] 104857600; (New-Object Random).NextBytes($out); " +
					"[System.IO.File]::WriteAllBytes('/mnt/volume1/testdata2.txt', $out)",
			}
			_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, cmdTestData...)
		} else {
			_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, "cp", testdataFile, fmt.Sprintf(
				"%v/%v:mnt/volume1/testdata", pod.Namespace, pod.Name))
		}
	}

	var cmd []string
	fsSize, err := getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	iosize := fsSize - spareSpace
	if len(size) != 0 {
		iosize = size[0]
	}
	iosize = iosize / 100 * 100 // will keep it as multiple of 100
	framework.Logf("Total IO size: %v", iosize)
	for i := int64(0); i < iosize; i = i + 100 {
		seek := fmt.Sprintf("%v", i)
		if wcpVsanDirectCluster {
			command := fmt.Sprintf("kubectl exec %s -n %s -- /bin/sh -c "+
				" dd if=/data0/testdata of=/mnt/file1 bs=1M count=100 seek=%s", pod.Name, pod.Namespace, seek)
			writeDataOnPodInSupervisor(sshWcpConfig, svcMasterIp, command)
		} else {
			var cmd []string
			if windowsEnv {
				cmd = []string{
					"exec",
					pod.Name,
					"--namespace=" + pod.Namespace,
					"powershell.exe -Command",
					"$inputFile = '/mnt/volume1/testdata2.txt'; " +
						"$outputFile = '/mnt/volume1/testdata3.txt'; " +
						"$blockSize = 1MB; " +
						"$count = 100; " +
						"$seek =" + seek +
						"$fs = [System.IO.File]::Open($outputFile, 'OpenOrCreate', 'Write'); " +
						"$fs.Seek($seek * $blockSize, [System.IO.SeekOrigin]::Begin) | Out-Null; " +
						"[byte[]]$buffer = New-Object byte[] $blockSize; " +
						"$input = [System.IO.File]::Open($inputFile, 'Open', 'Read'); " +
						"1..$count | ForEach-Object { " +
						"$bytesRead = $input.Read($buffer, 0, $blockSize); " +
						"if ($bytesRead -le 0) { break } " +
						"$fs.Write($buffer, 0, $bytesRead) }; " +
						"$input.Close(); " +
						"$fs.Close()",
				}
			} else {
				cmd = []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
					"/bin/sh", "-c", "dd if=/mnt/volume1/testdata of=/mnt/volume1/f1 bs=1M count=100 seek=" + seek}
			}
			_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, cmd...)
		}

	}

	if wcpVsanDirectCluster {
		cmd = []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
			"/bin/sh", "-c", "rm /data0/testdata"}
	} else {
		if windowsEnv {
			cmd = []string{
				"exec",
				pod.Name,
				"--namespace=" + pod.Namespace,
				"powershell.exe",
				"Remove-Item -Path '/mnt/volume1/testdata2.txt' " +
					"-Force",
			}
		} else {
			cmd = []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
				"/bin/sh", "-c", "rm /mnt/volume1/testdata"}
		}
	}
	_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, cmd...)

}

// writeDataOnPodInSupervisor writes data on supervisor pod by executing commands
// on svc master IP
func writeDataOnPodInSupervisor(sshClientConfig *ssh.ClientConfig, svcMasterIP string,
	cmd string) {

	res, err := sshExec(sshClientConfig, svcMasterIP,
		cmd)
	if err != nil || res.Code != 0 {
		fssh.LogResult(res)
		framework.Logf("contains: %v", strings.Contains(res.Stderr, "copied"))
		if !(strings.Contains(res.Stderr, "copied") || strings.Contains(res.Stderr, "Error from server: \n")) {
			framework.Failf("couldn't execute command: %s on host: %v , error: %s",
				cmd, svcMasterIP, err)
		}

	}

}

// verifyKnownDataInPod verify known data on a file in given pod's volume in 100mb loop
func verifyKnownDataInPod(f *framework.Framework, client clientset.Interface, pod *v1.Pod, testdataFile string,
	size ...int64) {
	var svcMasterIp string
	var sshWcpConfig *ssh.ClientConfig
	if wcpVsanDirectCluster {
		svcMasterIp = GetAndExpectStringEnvVar(svcMasterIP)
		svcMasterPwd := GetAndExpectStringEnvVar(svcMasterPassword)
		framework.Logf("svc master ip: %s", svcMasterIp)
		sshWcpConfig = &ssh.ClientConfig{
			User: rootUser,
			Auth: []ssh.AuthMethod{
				ssh.Password(svcMasterPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, "cp", testdataFile, fmt.Sprintf(
			"%v/%v:data0/testdata", pod.Namespace, pod.Name))
	} else {
		if windowsEnv {
			cmdTestData := []string{
				"exec",
				pod.Name,
				"--namespace=" + pod.Namespace,
				"powershell.exe",
				"Copy-Item -Path '/mnt/volume1/testdata2.txt' " +
					"-Destination '/mnt/volume1/testdata2_pod.txt'",
			}
			_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, cmdTestData...)
		} else {
			_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, "cp", testdataFile, fmt.Sprintf(
				"%v/%v:mnt/volume1/testdata", pod.Namespace, pod.Name))
		}
	}
	fsSize, err := getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	iosize := fsSize - spareSpace
	if len(size) != 0 {
		iosize = size[0]
	}
	iosize = iosize / 100 * 100 // will keep it as multiple of 100
	framework.Logf("Total IO size: %v", iosize)
	for i := int64(0); i < iosize; i = i + 100 {
		skip := fmt.Sprintf("%v", i)
		var cmd []string
		if wcpVsanDirectCluster {
			command := fmt.Sprintf("kubectl exec %s -n %s -- /bin/sh -c "+
				" dd if=/mnt/file1 of=/data0/testdata bs=1M count=100 skip=%s", pod.Name, pod.Namespace, skip)
			writeDataOnPodInSupervisor(sshWcpConfig, svcMasterIp, command)
		} else {
			if windowsEnv {
				cmd = []string{
					"exec",
					pod.Name,
					"--namespace=" + pod.Namespace,
					"powershell.exe -Command",
					"$inputFile = '/mnt/volume1/testdata3.txt'; " +
						"$outputFile = '/mnt/volume1/testdata2.txt'; " +
						"$blockSize = 1MB; " +
						"$count = 100; " +
						"$seek =10" +
						"$fs = [System.IO.File]::Open($outputFile, 'OpenOrCreate', 'Write'); " +
						"$fs.Seek($seek * $blockSize, [System.IO.SeekOrigin]::Begin) | Out-Null; " +
						"[byte[]]$buffer = New-Object byte[] $blockSize; " +
						"$input = [System.IO.File]::Open($inputFile, 'Open', 'Read'); " +
						"1..$count | ForEach-Object { " +
						"$bytesRead = $input.Read($buffer, 0, $blockSize); " +
						"if ($bytesRead -le 0) { break } " +
						"$fs.Write($buffer, 0, $bytesRead) }; " +
						"$input.Close(); " +
						"$fs.Close()",
				}
			} else {
				cmd = []string{"--namespace=" + pod.Namespace, "-c", pod.Spec.Containers[0].Name, "exec", pod.Name, "--",
					"/bin/sh", "-c", "dd if=/mnt/volume1/f1 of=/mnt/volume1/testdata bs=1M count=100 skip=" + skip}
			}
			_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, cmd...)
		}

		if wcpVsanDirectCluster {
			_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, "cp",
				fmt.Sprintf("%v/%v:/data0/testdata", pod.Namespace, pod.Name),
				testdataFile+pod.Name)
		} else {
			if windowsEnv {
				cmdTestData := []string{
					"exec",
					pod.Name,
					"--namespace=" + pod.Namespace,
					"powershell.exe",
					"Copy-Item -Path '/mnt/volume1/testdata2.txt' " +
						"-Destination '/mnt/volume1/testdata2_pod.txt'",
				}
				_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, cmdTestData...)
			} else {
				_ = e2ekubectl.RunKubectlOrDie(pod.Namespace, "cp",
					fmt.Sprintf("%v/%v:/mnt/volume1/testdata", pod.Namespace, pod.Name),
					testdataFile+pod.Name)
			}
		}

		framework.Logf("Running diff with source file and file from pod %v for 100M starting %vM", pod.Name, skip)
		if windowsEnv {
			cmdTestData := []string{
				"exec",
				pod.Name,
				"--namespace=" + pod.Namespace,
				"powershell.exe",
				"((Get-FileHash '/mnt/volume1/testdata2.txt' -Algorithm SHA256).Hash -eq " +
					"(Get-FileHash '/mnt/volume1/testdata2_pod.txt' -Algorithm SHA256).Hash)",
			}
			diffNotFound := strings.TrimSpace(e2ekubectl.RunKubectlOrDie(pod.Namespace, cmdTestData...))
			gomega.Expect(diffNotFound).To(gomega.Equal("True"))
		} else {
			op, err := exec.Command("diff", testdataFile, testdataFile+pod.Name).Output()
			framework.Logf("diff: %v", op)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(op)).To(gomega.BeZero())
		}
	}
}

// fillVolumesInPods fills the volumes in pods after leaving 100m for FS metadata
func fillVolumeInPods(f *framework.Framework, client clientset.Interface, pods []*v1.Pod) {
	for _, pod := range pods {
		size, err := getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		writeRandomDataOnPod(pod, size-100) // leaving 100m for FS metadata
	}
}

// Create files on volumes when pod attached to it
func createAndVerifyFilesOnVolume(namespace string, podname string,
	newEmptyfilesToCreate []string, filesToCheck []string) {
	createEmptyFilesOnVSphereVolume(namespace, podname, newEmptyfilesToCreate)
	ginkgo.By(fmt.Sprintf("Verify files exist on volume mounted on: %v", podname))
	verifyFilesExistOnVSphereVolume(namespace, podname, poll, pollTimeoutShort, filesToCheck...)
}
