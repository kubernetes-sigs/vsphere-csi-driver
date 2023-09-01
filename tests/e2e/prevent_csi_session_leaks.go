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
	"context"
	"regexp"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-block-vanilla][csi-file-vanilla][csi-supervisor]"+
	" Prevent CSI Session Leak Tests", func() {
	f := framework.NewDefaultFramework("session-leaks-csi")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client          clientset.Interface
		c               clientset.Interface
		csiReplicaCount int32
		deployment      *appsv1.Deployment
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		// Get CSI Controller's replica count from the setup
		c = client
		deployment, err = c.AppsV1().Deployments(csiSystemNamespace).Get(ctx,
			vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicaCount = *deployment.Spec.Replicas
	})

	ginkgo.It("Verify session creted with unique user agent", func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// check fss enabled for listview
		tasksListViewEnabled := isCsiFssEnabled(ctx, client, GetAndExpectStringEnvVar(envCSINamespace),
			"listview-tasks")
		// validate if sessions are created with uniuqe identifier in format "k8s-csi-useragent-<clusterid>-<feature_name>"
		gomega.Expect(tasksListViewEnabled).To(gomega.BeTrue())
		sessionList := getSessionList(ctx)
		matchRgx := "k8s-csi-useragent-[^.]+-[^.]+"
		count := validateVCSessions(sessionList, matchRgx)
		// expecting session are created with feature identifier appended (at least one expected)
		gomega.Expect(count).NotTo(gomega.Equal(0))
	})

	ginkgo.It("Verify session cleaned up on termination", func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer func() {
			// start CSI driver
			framework.Logf("Starting CSI driver")
			isServiceStarted, err := startCSIPods(ctx, client, csiReplicaCount, csiSystemNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isServiceStarted).NotTo(gomega.BeTrue())

		}()

		sessionList := getSessionList(ctx)
		matchRgx := "k8s-csi-useragent-[^.]+"
		count := validateVCSessions(sessionList, matchRgx)
		// stopping CSI Pods
		framework.Logf("Stopping CSI driver")
		isServiceStopped, err := stopCSIPods(ctx, client, csiSystemNamespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isServiceStopped).NotTo(gomega.BeTrue())
		// check sessions are cleaned up
		sessionList = getSessionList(ctx)
		updatedcount := validateVCSessions(sessionList, matchRgx)
		// expecting sessions are cleaned up after restarting CSI pods,
		// comparing sessions count before stopping csi pods with after pods stopped
		gomega.Expect(updatedcount == 0 || updatedcount < count).Should(gomega.BeTrue())
	})
})

// getSessionList will fetch all vc session created using govmomi client
func getSessionList(ctx context.Context) []vimtypes.UserSession {
	connect(ctx, &e2eVSphere)
	govmomiClient := newClient(ctx, &e2eVSphere)
	sm := govmomiClient.SessionManager
	var mgr mo.SessionManager
	pc := property.DefaultCollector(govmomiClient.Client)
	err := pc.RetrieveOne(ctx, sm.Reference(), []string{"sessionList"}, &mgr)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return mgr.SessionList
}

func validateVCSessions(sessionList []vimtypes.UserSession, matchString string) int {
	count := 0
	rgx, err := regexp.Compile(matchString)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, session := range sessionList {
		matched := rgx.MatchString(session.UserAgent)
		if matched {
			count++
		}
	}
	return count
}
