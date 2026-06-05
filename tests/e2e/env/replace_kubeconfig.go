package env

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"golang.org/x/crypto/ssh"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
)

// ReplaceSupervisorKubeconfig replaces the supervisor kubeconfig with admin.conf
func ReplaceSupervisorKubeconfig() {
	svcMasterIP := os.Getenv("SVC_MASTER_IP")
	svcMasterPwd := os.Getenv("SVC_MASTER_PASSWORD")
	if svcMasterIP == "" || svcMasterPwd == "" {
		return
	}

	kubeconfigPath := os.Getenv(constants.GcKubeConfigPath)
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv(constants.KubeconfigEnvVar)
	}
	if kubeconfigPath == "" {
		return
	}

	port := "22"
	ip := svcMasterIP
	if strings.Contains(ip, ":") {
		parts := strings.Split(ip, ":")
		ip = parts[0]
		port = parts[1]
	}
	addr := ip + ":" + port

	sshConfig := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password(svcMasterPwd),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshClient, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		fmt.Printf("ReplaceSupervisorKubeconfig: Failed to dial ssh: %v\n", err)
		return
	}
	defer sshClient.Close()

	sshSession, err := sshClient.NewSession()
	if err != nil {
		fmt.Printf("ReplaceSupervisorKubeconfig: Failed to create ssh session: %v\n", err)
		return
	}
	defer sshSession.Close()

	var bytesStdout bytes.Buffer
	sshSession.Stdout = &bytesStdout
	err = sshSession.Run("cat /etc/kubernetes/admin.conf")
	if err == nil && bytesStdout.String() != "" {
		err = os.WriteFile(kubeconfigPath, bytesStdout.Bytes(), 0644)
		if err != nil {
			fmt.Printf("ReplaceSupervisorKubeconfig: Failed to write kubeconfig: %v\n", err)
		} else {
			fmt.Printf("ReplaceSupervisorKubeconfig: Successfully replaced kubeconfig at %s with admin.conf\n", kubeconfigPath)
		}
	} else {
		fmt.Printf("ReplaceSupervisorKubeconfig: Failed to run cat /etc/kubernetes/admin.conf: %v\n", err)
	}
}
