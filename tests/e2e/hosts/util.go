package hosts

import (
	"context"
	"net"
	"time"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
)

// waitForHostToBeUp will check the status of hosts and also wait for
// pollTimeout minutes to make sure host is reachable.
func WaitForHostToBeUp(vs *config.E2eTestConfig, ip string, pollInfo ...time.Duration) error {
	framework.Logf("checking host status of %v", ip)
	pollTimeOut := constants.HealthStatusPollTimeout
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
	ip, portNum, err := env.GetPortNumAndIP(&vs.TestInput.TestBedInfo, ip)
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
