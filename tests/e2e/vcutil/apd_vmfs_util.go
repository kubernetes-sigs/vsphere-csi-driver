package vcutil

import (
	"context"
	"fmt"
	"net"
	"time"

	cryptoSsh "golang.org/x/crypto/ssh"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
)

// This function inject APD in given VMFS datastore
func InjectAPDToVMFS(ctx context.Context,
	e2eTestConfig *config.E2eTestConfig,
	scsi string,
	hostIp string) (success bool) {
	executed := false
	enableErrorInjectCmd := constants.VSISH_SET + constants.INJECT_ERROR
	injectAPDCmd := constants.VSISH_SET + constants.STORAGE_PATH + scsi + constants.ERROR + constants.INJECT_APD_CODE
	RunSsh(ctx, enableErrorInjectCmd,
		e2eTestConfig, hostIp)
	RunSsh(ctx, injectAPDCmd,
		e2eTestConfig, hostIp)
	return executed
}

// This function clear APD in given VMFS datastore
func ClearAPDToVMFS(ctx context.Context,
	e2eTestConfig *config.E2eTestConfig,
	scsi string,
	hostIp string) (success bool) {
	executed := false
	disableErrorInject := constants.VSISH_SET + constants.CLEAR_ERROR
	removeAPDCommand := constants.VSISH_SET + constants.STORAGE_PATH + scsi + constants.ERROR + constants.CLEAR_APD_CODE

	_, err := RunSsh(ctx, disableErrorInject,
		e2eTestConfig, hostIp)
	if err != nil {
		fmt.Printf("Got error while enabling error injection : %v\n", err)
		return executed
	}
	_, err = RunSsh(ctx, removeAPDCommand,
		e2eTestConfig, hostIp)
	if err != nil {
		fmt.Printf("Got error while enabling error injection : %v\n", err)
		return executed
	}
	return true
}

// This function invokes reboot command on the given vCenter over SSH.
func RunSsh(ctx context.Context, sshCmd string, e2eTestConfig *config.E2eTestConfig, host string) ([]byte, error) {
	fmt.Printf("Running sshCmd %s on host %s\n", sshCmd, host)
	config := &cryptoSsh.ClientConfig{
		User: "root", // Replace with your SSH username
		Auth: []cryptoSsh.AuthMethod{
			cryptoSsh.KeyboardInteractive(func(user, instruction string, questions []string, echos []bool) (answers []string, err error) {
				answers = make([]string, len(questions))
				nimbusGeneratedEsxPwd := e2eTestConfig.TestInput.Global.Password
				for n := range questions {
					answers[n] = nimbusGeneratedEsxPwd
				}
				return answers, nil
			}),
		},
		HostKeyCallback: cryptoSsh.InsecureIgnoreHostKey(), // Not recommended for production environments. You should verify the host key.
		Timeout:         5 * time.Second,
	}
	addr := net.JoinHostPort(host, constants.DefaultSshPort)
	client, err := cryptoSsh.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// Create a new session
	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	// Execute a command on the remote server
	output, err := session.CombinedOutput(sshCmd) // Replace with the command you want to execute.
	if err != nil {
		return nil, err
	}
	fmt.Printf("%s", output)
	return output, err
}
