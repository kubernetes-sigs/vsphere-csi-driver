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
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var _ = ginkgo.Describe("[vsan-stretch-vanilla] vsan stretched cluster tests", func() {
	f := framework.NewDefaultFramework("vsan-stretch")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	const defaultVolumeOpsScale = 30
	const operationStormScale = 50
	var (
		client                     clientset.Interface
		namespace                  string
		nodeList                   *v1.NodeList
		storagePolicyName          string
		scParameters               map[string]string
		storageClassName           string
		csiNs                      string
		fullSyncWaitTime           int
		volumeOpsScale             int
		storageThickPolicyName     string
		labelKey                   string
		labelValue                 string
		sshClientConfig            *ssh.ClientConfig
		pandoraSyncWaitTime        int
		defaultDatacenter          *object.Datacenter
		defaultDatastore           *object.Datastore
		isVsanHealthServiceStopped bool
		nimbusGeneratedK8sVmPwd    string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		readVcEsxIpsViaTestbedInfoJson(GetAndExpectStringEnvVar(envTestbedInfoJsonPath))
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		csiNs = GetAndExpectStringEnvVar(envCSINamespace)
		isVsanHealthServiceStopped = false

		initialiseFdsVar(ctx)
		err = waitForAllNodes2BeReady(ctx, client)
		framework.ExpectNoError(err, "cluster not completely healthy")

		// TODO: verify csi pods are up

		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		storageThickPolicyName = os.Getenv(envStoragePolicyNameWithThickProvision)
		if storageThickPolicyName == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		framework.Logf("Storageclass is %v and err is %v", sc, err)
		if (!strings.Contains(err.Error(), "not found")) && sc != nil {
			framework.ExpectNoError(client.StorageV1().StorageClasses().Delete(ctx, defaultNginxStorageClassName,
				*metav1.NewDeleteOptions(0)), "Unable to delete storage class "+defaultNginxStorageClassName)
		}
		scParameters = make(map[string]string)
		nodeList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Unable to list k8s nodes")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find k8s nodes")
		}
		if os.Getenv("VOLUME_OPS_SCALE") != "" {
			volumeOpsScale, err = strconv.Atoi(os.Getenv(envVolumeOperationsScale))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			volumeOpsScale = defaultVolumeOpsScale
		}
		framework.Logf("VOLUME_OPS_SCALE is set to %d", volumeOpsScale)

		labelKey = "app"
		labelValue = "e2e-labels"

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		var datacenters []string
		datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		err = client.StorageV1().StorageClasses().Delete(ctx, storageClassName, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if isVsanHealthServiceStopped {
			vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
			ginkgo.By(fmt.Sprintf("Starting %v on the vCenter host", vsanhealthServiceName))
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
	})

	ginkgo.JustAfterEach(func() {
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			powerOnHostParallel(fds.hostsDown)
			fds.hostsDown = nil
		}
		if len(fds.hostsPartitioned) > 0 && fds.hostsPartitioned != nil {
			toggleNetworkFailureParallel(fds.hostsPartitioned, false)
			fds.hostsPartitioned = nil
		}
		fds.primarySiteHosts = nil
		fds.secondarySiteHosts = nil
	})

	/*
		Primary site down
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create a statefulset, deployment with volumes from the stretched datastore
		3.	Bring down the primary site
		4.	Verify that the VMs hosted by esx servers are brought up on the other site
		5.	Verify that the k8s cluster is healthy and all the k8s constructs created in step 2 are running and volume
			and application lifecycle actions work fine
		6.	Bring primary site up and wait for testbed to be back to normal
		7.	Delete all objects created in step 2 and 5
	*/
	ginkgo.It("[primary-centric] Primary site down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		ginkgo.By("Creating statefulset and deployment with volumes from the stretched datastore")
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		replicas := *(statefulset.Spec.Replicas)
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		ginkgo.By("Bring down the primary site")
		siteFailover(true)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)
		// Scale down replicas of statefulset and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			ssPodsBeforeScaleDown, replicas-1, true, true)
		// Scale up replicas of statefulset and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas, true, true)

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

	})

	/*
	   Statefulset scale up/down while primary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create two statefulset with replica count 1(sts1) and 5(sts2) respectively using a thick provision policy
	       and wait for all replicas to be running
	   3.  Change replica count of sts1 and sts2 to 3
	   4.  Bring down primary site
	   5.  Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
	   6.  Verify there were no issue with replica scale up/down and verify pod entry in CNS volumemetadata for the
	       volumes associated with the PVC used by statefulsets are updated
	   7.  Change replica count of sts1 to 5 a sts2 to 1 and verify they are successful
	   8.  Delete statefulsets and its pvcs created in step 2
	   9.  Bring primary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("[primary-centric][control-plane-on-primary] Statefulset scale up/down while primary"+
		" site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulsets sts1 with replica count 1 and sts2 with 5 and wait for all" +
			"the replicas to be running")
		statefulset1, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 1, "web")
		replicas1 := *(statefulset1.Spec.Replicas)
		statefulset2, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 5, "web-nginx")
		ss2PodsBeforeScaleDown := fss.GetPodList(client, statefulset2)
		replicas2 := *(statefulset2.Spec.Replicas)

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas1 += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulset %v to number of Replica: %v", statefulset1.Name, replicas1))
		fss.UpdateReplicas(client, statefulset1, replicas1)

		replicas2 -= 2
		ginkgo.By(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v", statefulset2.Name, replicas2))
		fss.UpdateReplicas(client, statefulset2, replicas2)

		ginkgo.By("Bring down the primary site")
		siteFailover(true)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying statefulset scale up/down went fine on sts1 and sts2")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, false, true)
		// Scale down replicas of statefulset2 and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, false, true)

		// Scaling up statefulset sts1
		replicas1 += 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, true, false)

		// Scaling down statefulset sts2
		replicas2 -= 2
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, true, false)

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, claim := range pvcs.Items {
			pv := getPvFromClaim(client, namespace, claim.Name)
			err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeHandle))
		}

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Pod deletion while primary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create 30 PVCs and wait for its binding of each pvc with a PV
	   3.  Create a pod with each PVC created in step 2
	   4.  Delete the pod created in step 3
	   5.  Bring down primary site
	   6.  Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
	   7.  wait for full sync
	   8.  Verify volume attachment are also deleted for the pods and pod entry in CNS volumemetadata for the
	       volume associated with the PVC created in step 2 is removed
	   9.  Bring primary site up and wait for testbed to be back to normal
	   10. Delete the PVCs created in step 2

	*/
	ginkgo.It("[primary-centric][control-plane-on-primary] Pod deletion while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err := strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}
		var pods []*v1.Pod
		var pvclaims []*v1.PersistentVolumeClaim = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc")
			pvclaims[i], err = createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		ginkgo.By("Create pods")
		for i := 0; i < volumeOpsScale; i++ {
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[i]}, false, execCommand)
			framework.Logf("Created pod %s", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		}
		defer func() {
			for _, pod := range pods {
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while deleting pods")
		var wg sync.WaitGroup
		wg.Add(2)
		go deletePodsInParallel(client, namespace, pods, &wg)
		go siteFailureInParallel(true, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Sleeping full-sync interval for all the pod Metadata " +
			"to be deleted")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		ginkgo.By("Verify volume is detached from the node")
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pods[i].Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pods[i].Spec.NodeName))

		}
		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   PVC creation while primary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create 30 PVCs using a thick provision policy so that it takes some time for PVC creation to go through
	   3.  Bring down primary site
	   4.  Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
	   5.  Verify that the PVCs created in step 2 is bound successfully
	   6.  Bring primary site up and wait for testbed to be back to normal
	   7.  Delete PVCs created in step 2
	*/
	ginkgo.It("[primary-centric] PVC creation while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"
		var pvclaims []*v1.PersistentVolumeClaim

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while creating pvcs")
		var wg sync.WaitGroup
		ch := make(chan *v1.PersistentVolumeClaim)
		lock := &sync.Mutex{}
		wg.Add(2)
		go createPvcInParallel(client, namespace, diskSize, sc, ch, lock, &wg, volumeOpsScale)
		go func() {
			for v := range ch {
				pvclaims = append(pvclaims, v)
			}
		}()
		go siteFailureInParallel(true, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		defer func() {
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		for _, pvclaim := range pvclaims {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// TODO: List orphan volumes

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Primary site network isolation
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create a statefulset, deployment with volumes from the stretched datastore
		3.	Isolate primary site from witness and secondary site
		4.	Verify that the VMs hosted by esx servers are brought up on the other site
		5.	Verify that the k8s cluster is healthy and all the k8s constructs created in step 2 are running
			and volume and application lifecycle actions work fine
		6.	Re-establish primary site network and wait for testbed to be back to normal
		7.	Delete all objects created in step 2
	*/
	ginkgo.It("[primary-centric][control-plane-on-primary][distributed] Primary site network isolation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		ginkgo.By("Creating statefulset and deployment with volumes from the stretched datastore")
		statefulset, deployment, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Cause a network failure on primary site
		ginkgo.By("Isolate primary site from witness and secondary site")
		siteNetworkFailure(true, false)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			siteNetworkFailure(true, true)
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after network failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking if volumes and pods post network failure are healthy")
		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)
		// Scale down replicas of statefulset and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			ssPodsBeforeScaleDown, replicas-1, true, true)
		// Scale up replicas of statefulset and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas, true, true)

		ginkgo.By("Bring up the primary site")
		siteNetworkFailure(true, true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, claim := range pvcs.Items {
			pv := getPvFromClaim(client, namespace, claim.Name)
			err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeHandle))
		}
	})

	/*
		PVC deletion while primary site goes down
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create 30 PVCs and wait for each pvc to bind to its PV
		3.	Delete the PVCs created in step2
		4.	Bring down primary site
		5.	Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
		6.	Verify PVs and CNS volumes associated with PVCs created in step 2 are also deleted successfully
		7.	Bring primary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("[primary-centric] PVC deletion while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc %v", i)
			pvclaims[i], err = createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while deleting pvcs")
		var wg sync.WaitGroup
		wg.Add(2)
		go deletePvcInParallel(client, pvclaims, namespace, &wg)
		go siteFailureInParallel(true, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			if !apierrors.IsNotFound(err) {
				framework.Logf("Persistent Volume %v still not deleted with err %v", pv.Name, err)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				// Orphan volumes may be left over here, hence logging those PVs and ignoring the error for now.
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				framework.Logf("Volume %v still not deleted from CNS with err %v", pv.Name, err)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}
		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		siteRestore(true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Pod creation while primary site goes down
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create 30 PVCs and wait for each pvc to bind to its PV
		3.	Create a POD using each PVC created in step 2
		4.	Bring down primary site
		5.	Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
		6.	Verify that the pod created in step 4 come up successfully
		7.	Bring primary site up and wait for testbed to be back to normal
		8.	Delete pods created in step 3
		9.	Delete PVCs created in step 2

	*/
	ginkgo.It("[primary-centric][control-plane-on-primary] Pod creation while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pods []*v1.Pod
		var pvclaims []*v1.PersistentVolumeClaim = make([]*v1.PersistentVolumeClaim, volumeOpsScale)

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc %v", i)
			pvclaims[i], err = createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		defer func() {
			for _, pvclaim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim = nil
			}
		}()

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		/// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while creating pods")
		var wg sync.WaitGroup
		wg.Add(2)
		ch := make(chan *v1.Pod)
		lock := &sync.Mutex{}
		go createPodsInParallel(client, namespace, pvclaims, ctx, lock, ch, &wg, volumeOpsScale)
		go func() {
			for v := range ch {
				pods = append(pods, v)
			}
		}()
		go siteFailureInParallel(true, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(true)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking whether pods are in Running or ExitCode:0 state")
		for _, pod := range pods {
			framework.Logf("Pod is %s", pod.Name)
			err = waitForPodsToBeInErrorOrRunning(client, pod.Name, namespace, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pod := range pods {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
	   Label updates to PV, PVC, pod while primary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create 30 PVCs and wait for them to be bound
	   3.  Add labels to the PVs, PVCs
	   4.  Bring down primary site
	   5.  Verify that the VMs on the primary site are started up on the other esx servers in the secondary site
	   6.  Wait for full sync
	   7.  Verify CNS entries
	   8.  Delete the PVCs created in step 2
	   9.  Bring primary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("[primary-centric] Label updates to PV, PVC, pod while primary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err := strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc")
			pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
			pvclaims = append(pvclaims, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the primary site while adding labels to PVCs and PVs")
		var wg sync.WaitGroup
		labels := make(map[string]string)
		labels[labelKey] = labelValue
		wg.Add(3)
		go updatePvcLabelsInParallel(ctx, client, namespace, labels, pvclaims, &wg)
		go updatePvLabelsInParallel(ctx, client, namespace, labels, persistentvolumes, &wg)
		go siteFailureInParallel(true, &wg)
		wg.Wait()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Sleeping full-sync interval for volumes to be updated " +
			"with labels in CNS")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		for _, pvc := range pvclaims {
			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
				labels, pvc.Name, namespace))
			pv := getPvFromClaim(client, namespace, pvc.Name)
			err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pv := range persistentvolumes {
			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s",
				labels, pv.Name))
			err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pvclaim := range pvclaims {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the primary site")
		siteRestore(true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
	/*
		PVC creation while secondary site goes down and csi provisioner leader is in secondary site
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.  Ensure csi-provisioner leader is in secondary site
		3.  Create 30 PVCs using a thick provision policy so that it takes some time for PVC creation to go through
		4.  Bring down secondary site
		5.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
		6.  Verify that the PVCs created in step 3 is bound successfully
		7.  Bring secondary site up and wait for testbed to be back to normal
		9.  Delete PVC created in step 3
		10. If there is an orphan volume clean up that using cnsctl
	*/
	ginkgo.It("[distributed] PVC creation while secondary site goes down"+
		" and csi provisioner leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"
		var pvclaims []*v1.PersistentVolumeClaim

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", provisionerContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, provisionerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while creating pvcs")
		var wg sync.WaitGroup
		ch := make(chan *v1.PersistentVolumeClaim)
		lock := &sync.Mutex{}
		wg.Add(2)
		go createPvcInParallel(client, namespace, diskSize, sc, ch, lock, &wg, volumeOpsScale)
		go func() {
			for v := range ch {
				pvclaims = append(pvclaims, v)
			}
		}()
		go siteFailureInParallel(false, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			for _, pvclaim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		for _, pvclaim := range pvclaims {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// TODO: List orphan volumes

		ginkgo.By("Bring up the secondary site")
		siteRestore(false)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		PVC deletion while secondary site goes down and csi provisioner leader is in secondary site
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.  Ensure csi-provisioner leader is in secondary site
		3.  Create 30 PVCs and wait for each PVC binding with a PV
		4.	Delete PVCs created in step 3
		5.  Bring down secondary site
		6.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
		7.  Verify PV and CNS volumes associated with PVC created in step 2 are also deleted successfully
		8.  Bring secondary site up and wait for testbed to be back to normal

	*/
	ginkgo.It("[distributed] PVC deletion while secondary site goes down"+
		" and csi provisioner leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", provisionerContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, provisionerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc %v", i)
			pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = append(pvclaims, pvc)
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while deleting pvcs")
		var wg sync.WaitGroup
		wg.Add(2)
		go deletePvcInParallel(client, pvclaims, namespace, &wg)
		go siteFailureInParallel(false, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(false)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			if !apierrors.IsNotFound(err) {
				framework.Logf("Persistent Volume %v still not deleted with err %v", pv.Name, err)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				// Orphan volumes may be left over here, hence logging those PVs and ignoring the error for now.
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				framework.Logf("Volume %v still not deleted from CNS with err %v", pv.Name, err)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// TODO: List orphan volumes
			}

		}
		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(false)
			fds.hostsDown = []string{}
		}

		siteRestore(true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Pod creation while secondary site goes down and csi attacher leader is in secondary site
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.  Ensure csi-attacher leader is in secondary site
		3.  Create 30 PVCs and wait for each PVC binding with a PV
		4.	Create PODs using PVCs created in step 3
		5.  Bring down secondary site
		6.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
		7.	Verify that the pods created in step 4 come up successfully
		8.  Delete pods created in step 4 and 8
		9.  Bring secondary site up and wait for testbed to be back to normal
		10.	Delete PVCs created in step 2

	*/
	ginkgo.It("[distributed] Pod creation while secondary site goes down"+
		" and csi attacher leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim
		var pods []*v1.Pod

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", attacherContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, attacherContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc %v", i)
			pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = append(pvclaims, pvc)
		}

		defer func() {
			for _, pvclaim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pvclaim = nil
			}
		}()

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		/// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while creating pods")
		var wg sync.WaitGroup
		wg.Add(2)
		ch := make(chan *v1.Pod)
		lock := &sync.Mutex{}
		go createPodsInParallel(client, namespace, pvclaims, ctx, lock, ch, &wg, volumeOpsScale)
		go func() {
			for v := range ch {
				pods = append(pods, v)
			}
		}()
		go siteFailureInParallel(false, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(false)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking whether pods are in Running or ExitCode:0 state")
		for _, pod := range pods {
			framework.Logf("Pod is %s", pod.Name)
			err = waitForPodsToBeInErrorOrRunning(client, pod.Name, namespace, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(false)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pod := range pods {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		Pod deletion while secondary site goes down and csi attacher leader is in secondary site
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.  Ensure csi-attacher leader is in secondary site
		3.  Create 30 PVCs and wait for each PVC binding with a PV
		4.	Create PODs using PVCs created in step 3
		5.	Delete pods created in step 4
		6.  Bring down secondary site
		7.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
		8.	Verify that the pods get deleted successfully
		9.  Verify volumeattachments are also deleted
		10.	Verify CNS volume metadata for the volumes
		11. Bring secondary site up and wait for testbed to be back to normal
		12.	Delete PVCs created in step 2

	*/
	ginkgo.It("[distributed] Pod deletion while secondary site goes down"+
		" and csi attacher leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim
		var pods []*v1.Pod

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", attacherContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, attacherContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc")
			pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaims = append(pvclaims, pvc)
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		ginkgo.By("Create pods")
		for i := 0; i < volumeOpsScale; i++ {
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaims[i]}, false, execCommand)
			framework.Logf("Created pod %s", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		}
		defer func() {
			for _, pod := range pods {
				err = fpod.DeletePodWithWait(client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while deleting pods")
		var wg sync.WaitGroup
		wg.Add(2)
		go deletePodsInParallel(client, namespace, pods, &wg)
		go siteFailureInParallel(false, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(false)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pod := range pods {
			framework.Logf("Wait up to %v for pod %q to be fully deleted", pollTimeout, pod.Name)
			err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Verify volume is detached from the node")
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pods[i].Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pods[i].Spec.NodeName))

		}
		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(false)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Secondary site down
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Create a statefulset, deployment with volumes from the stretched datastore
		3.	Bring down the secondary site
		4.	Verify that the VMs hosted by esx servers are brought up on the other site
		5.	Verify that the k8s cluster is healthy and all the k8s constructs created in step 2 are running and volume
			and application lifecycle actions work fine
		6.	Bring secondary site up and wait for testbed to be back to normal
		7.	Delete all objects created in step 2 and 5
	*/
	ginkgo.It("[control-plane-on-primary] Secondary site down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset and deployment with volumes from the stretched datastore")
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		replicas := *(statefulset.Spec.Replicas)

		defer func() {
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site")
		siteFailover(false)

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(false)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)
		// Scale down replicas of statefulset and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			ssPodsBeforeScaleDown, replicas-1, true, true)
		// Scale up replicas of statefulset and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas, true, true)

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(false)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
	})

	/*
		Network failure between sites
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	create a statefulsets sts1 with replica count 1 and sts2 with 5 and wait for all
			the replicas to be running
		3.	Change replica counts of sts1 ans sts 2 to 3 replicas
		4.	Cause a network failure between primary and secondary site
		5.	Verify that all the k8s VMs are brought up on the primary site
		6.	Verify that the k8s cluster is healthy and statefulset scale up/down went fine and
			volume and application lifecycle actions work fine
		7.	Change replica count of both sts1 and sts2 to 5
		8.	Restore the network between primary and secondary site
		9.	Wait for k8s-master VM pinned to secondary site to come up and wait for any other VMs
			which are moving to secondary site as well
		10.	Verify that the k8s cluster is healthy and statefulset scale up/down went fine and
			volume and application lifecycle actions work fine
		11.	Cleanup all objects created so far in the test
	*/
	ginkgo.It("[control-plane-on-primary][distributed] Network failure between sites", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()
		framework.Logf("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulsets sts1 with replica count 1 and sts2 with 5 and wait for all" +
			"the replicas to be running")
		statefulset1, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 1, "web")
		replicas1 := *(statefulset1.Spec.Replicas)
		statefulset2, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 5, "web-nginx")
		ss2PodsBeforeScaleDown := fss.GetPodList(client, statefulset2)
		replicas2 := *(statefulset2.Spec.Replicas)

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas1 += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulset %v to number of Replica: %v", statefulset1.Name, replicas1))
		fss.UpdateReplicas(client, statefulset1, replicas1)

		replicas2 -= 2
		ginkgo.By(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v", statefulset2.Name, replicas2))
		fss.UpdateReplicas(client, statefulset2, replicas2)

		ginkgo.By("Isolate secondary site from witness and primary site")
		siteNetworkFailure(false, false)

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsPartitioned) > 0 && fds.hostsPartitioned != nil {
				siteNetworkFailure(false, true)
				fds.hostsPartitioned = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying statefulset scale up/down went fine on sts1 and sts2")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, false, true)
		// Scale down replicas of statefulset2 and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, false, true)

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)

		// Scaling up statefulset sts1
		replicas1 += 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, true, false)

		// Scaling down statefulset sts2
		replicas2 -= 2
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, true, false)

		ginkgo.By("Bring up the secondary site by removing network failure")
		if len(fds.hostsPartitioned) > 0 && fds.hostsPartitioned != nil {
			siteNetworkFailure(false, true)
			fds.hostsPartitioned = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

	})

	/*
		Witness failure
		Steps:
		1.	Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Bring down the witness host
		3.	Run volume and application lifecycle actions, verify provisioning goes through
			but VM and storage compliance are false.
		4.	Bring the witness host up
		5.	Run volume and application lifecycle actions
		6.	Cleanup all objects created in step 3 and 5
	*/
	ginkgo.It("[distributed] Witness failure", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down witness host")
		toggleWitnessPowerState(true)

		defer func() {
			ginkgo.By("Bring up the witness host before terminating the test")
			if fds.witnessDown != "" {
				toggleWitnessPowerState(false)
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, true, false, 0, "")
		replicas := *(statefulset.Spec.Replicas)
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		// Scale down replicas of statefulset and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			ssPodsBeforeScaleDown, replicas-1, true, true)

		comp := checkVmStorageCompliance(client, storagePolicyName)
		if !comp {
			framework.Failf("Expected VM and storage compliance to be false but found true")
		}

		ginkgo.By("Bring up witness host")
		if fds.witnessDown != "" {
			toggleWitnessPowerState(false)
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)

		ginkgo.By(fmt.Sprintf("Scaling up statefulset: %v to number of Replica: %v",
			statefulset.Name, replicas))
		// Scale up replicas of statefulset and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas, true, true)

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
	})

	/*
	   Statefulset scale up/down while secondary site goes down when csi provisioner and
	   attacher leaders are in secondary site
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Ensure csi-attacher and csi-provisioner have leaders is in secondary site
	   3.  Create two statefulset with replica count 1(sts1) and 5(sts2) respectively using a thick provision policy
	       and wait for all replicas to be running
	   4.  Change replica count of sts1 and sts2 to 3
	   5.  Bring down secondary site
	   6.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
	   7.  Verify there were no issue with replica scale up/down and verify pod entry in CNS volumemetadata for the
	       volumes associated with the PVC used by statefulsets are updated
	   8.  Change replica count of sts1 to 5 a sts2 to 1 and verify they are successful
	   9.  Delete statefulsets and its pvcs created in step 2
	   10. Bring secondary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("[distributed] Statefulset scale up/down while secondary site goes down when csi provisioner"+
		" and attacher leaders are in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", provisionerContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, provisionerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Ensuring %s leader is in secondary site", attacherContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, attacherContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulsets sts1 with replica count 1 and sts2 with 5 and wait for all" +
			"the replicas to be running")
		statefulset1, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 1, "web")
		replicas1 := *(statefulset1.Spec.Replicas)
		statefulset2, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 5, "web-nginx")
		ss2PodsBeforeScaleDown := fss.GetPodList(client, statefulset2)
		replicas2 := *(statefulset2.Spec.Replicas)

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas1 += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulset %v to number of Replica: %v", statefulset1.Name, replicas1))
		fss.UpdateReplicas(client, statefulset1, replicas1)

		replicas2 -= 2
		ginkgo.By(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v", statefulset2.Name, replicas2))
		fss.UpdateReplicas(client, statefulset2, replicas2)

		ginkgo.By("Bring down the secondary site")
		siteFailover(false)

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(false)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying statefulset scale up/down went fine on sts1 and sts2")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, false, true)
		// Scale down replicas of statefulset2 and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, false, true)

		// Scaling up statefulset sts1
		replicas1 += 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, true, false)

		// Scaling down statefulset sts2
		replicas2 -= 2
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, true, false)

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, claim := range pvcs.Items {
			pv := getPvFromClaim(client, namespace, claim.Name)
			err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeHandle))
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(false)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Label updates to PV, PVC while primary site goes down
	   when syncer pod leader is in secondary site
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Ensure syncer leader is in secondary site
	   3.  Create 30 PVCs and wait for them to be bound
	   4.  Add labels to the PVs, PVCs
	   5.  Bring down secondary site
	   6.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
	   7.  Wait for full sync
	   8.  Verify CNS entries
	   9.  Delete the PVCs created in step 2
	   10.  Bring secondary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("[distributed] Label updates to PV, PVC while primary site goes down"+
		" when syncer pod leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err := strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", syncerContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, syncerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc")
			pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
			pvclaims = append(pvclaims, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while adding labels to PVCs and PVs")
		var wg sync.WaitGroup
		labels := make(map[string]string)
		labels[labelKey] = labelValue

		wg.Add(3)
		go updatePvcLabelsInParallel(ctx, client, namespace, labels, pvclaims, &wg)
		go updatePvLabelsInParallel(ctx, client, namespace, labels, persistentvolumes, &wg)
		go siteFailureInParallel(false, &wg)
		wg.Wait()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Sleeping full-sync interval for volumes to be updated " +
			"with labels in CNS")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		for _, pvc := range pvclaims {
			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pvc %s in namespace %s",
				labels, pvc.Name, namespace))
			pv := getPvFromClaim(client, namespace, pvc.Name)
			err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pv := range persistentvolumes {
			ginkgo.By(fmt.Sprintf("Waiting for labels %+v to be updated for pv %s",
				labels, pv.Name))
			err = e2eVSphere.waitForLabelsToBeUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pvclaim := range pvclaims {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the primary site")
		siteRestore(false)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
	   Statefulset scale up/down while secondary site goes down when csi driver leader is in secondary site
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Ensure csi-driver leader is in secondary site
	   3.  Create two statefulset with replica count 1(sts1) and 5(sts2) respectively using a thick provision policy
	       and wait for all replicas to be running
	   4.  Change replica count of sts1 and sts2 to 3
	   5.  Bring down secondary site
	   6.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
	   7.  Verify there were no issue with replica scale up/down and verify pod entry in CNS volumemetadata for the
	       volumes associated with the PVC used by statefulsets are updated
	   8.  Change replica count of sts1 to 5 a sts2 to 1 and verify they are successful
	   9.  Delete statefulsets and its pvcs created in step 2
	   10. Bring secondary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("[distributed] Statefulset scale up/down while secondary site goes down"+
		" when csi driver leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"
		csiPodOnSite, nodeName := "", ""
		ignoreLabels := make(map[string]string)

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", csiDriverContainerName)
		// Fetch master ip present on that site
		masterIpOnSite, err := getMasterIpOnSite(ctx, client, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Fetch the name of master node on that site from the IP address
		k8sNodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, node := range k8sNodes.Items {
			addrs := node.Status.Addresses
			for _, addr := range addrs {
				if addr.Type == v1.NodeInternalIP && (net.ParseIP(addr.Address)).To4() != nil &&
					addr.Address == masterIpOnSite {
					nodeName = node.Name
					break
				}
			}
		}

		// Get the name pf csi controller pod running on master node on that site
		csiPods, err := fpod.GetPodsInNamespace(client, csiSystemNamespace, ignoreLabels)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, csiPod := range csiPods {
			if strings.Contains(csiPod.Name, vSphereCSIControllerPodNamePrefix) &&
				csiPod.Spec.NodeName == nodeName {
				csiPodOnSite = csiPod.Name
			}
		}
		// Delete csi controller pods on other masters which is not present on that site
		deleteCsiControllerPodOnOtherMasters(client, csiPodOnSite)
		masterIpOnSecSite, err := getMasterIpOnSite(ctx, client, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		allCsiContainerNames := []string{syncerContainerName, provisionerContainerName, attacherContainerName,
			resizerContainerName, snapshotterContainerName}

		for _, containerName := range allCsiContainerNames {
			_, masterIp, err := getK8sMasterNodeIPWhereContainerLeaderIsRunning(ctx, client,
				sshClientConfig, containerName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if masterIp != masterIpOnSecSite {
				framework.Failf("couldn't get :%s container on master node ip: %s",
					containerName, masterIpOnSite)
			}
		}

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulsets sts1 with replica count 1 and sts2 with 5 and wait for all" +
			"the replicas to be running")
		statefulset1, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 1, "web")
		replicas1 := *(statefulset1.Spec.Replicas)
		statefulset2, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 5, "web-nginx")
		ss2PodsBeforeScaleDown := fss.GetPodList(client, statefulset2)
		replicas2 := *(statefulset2.Spec.Replicas)

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas1 += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulset %v to number of Replica: %v", statefulset1.Name, replicas1))
		fss.UpdateReplicas(client, statefulset1, replicas1)

		replicas2 -= 2
		ginkgo.By(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v", statefulset2.Name, replicas2))
		fss.UpdateReplicas(client, statefulset2, replicas2)

		ginkgo.By("Bring down the secondary site")
		siteFailover(false)

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(false)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying statefulset scale up/down went fine on sts1 and sts2")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, false, true)
		// Scale down replicas of statefulset2 and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, false, true)

		// Scaling up statefulset sts1
		replicas1 += 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, true, false)

		// Scaling down statefulset sts2
		replicas2 -= 2
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, true, false)

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, claim := range pvcs.Items {
			pv := getPvFromClaim(client, namespace, claim.Name)
			err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeHandle))
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(false)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
	   Statefulset scale up/down while secondary site goes down
	   Steps:
	   1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
	   2.  Create two statefulset with replica count 1(sts1) and 5(sts2) respectively
	   	   and wait for all replicas to be running
	   3.  Change replica count of sts1 and sts2 to 3
	   4.  Bring down secondary site
	   5.  Change replica count of sts1 to 5 and sts2 to 1
	   6.  Verify that the VMs on the secondary site are started up on the other esx servers
	       in the primary site
	   7.  Verify there were no issue with replica scale up/down and verify pod entry in CNS volumemetadata
	   	   for the volume associated with the PVC used by statefulsets are updated
	   8.  Change replica count of sts1 and sts2 to 3 and verify they are successful
	   9.  Delete statefulsets created in step 2
	   10. Delete the PVCs used by statefulsets in step 2
	   11. Bring secondary site up and wait for testbed to be back to normal
	*/
	ginkgo.It("[control-plane-on-primary] Statefulset scale up/down while secondary site goes down", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulsets sts1 with replica count 1 and sts2 with 5 and wait for all" +
			"the replicas to be running")
		statefulset1, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 1, "web")
		replicas1 := *(statefulset1.Spec.Replicas)
		statefulset2, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 5, "web-nginx")
		ss2PodsBeforeScaleDown := fss.GetPodList(client, statefulset2)
		replicas2 := *(statefulset2.Spec.Replicas)

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		replicas1 += 2
		ginkgo.By(fmt.Sprintf("Scaling up statefulset %v to number of Replica: %v", statefulset1.Name, replicas1))
		fss.UpdateReplicas(client, statefulset1, replicas1)

		replicas2 -= 2
		ginkgo.By(fmt.Sprintf("Scaling down statefulset: %v to number of Replica: %v", statefulset2.Name, replicas2))
		fss.UpdateReplicas(client, statefulset2, replicas2)

		ginkgo.By("Bring down the secondary site")
		siteFailover(false)

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				siteRestore(false)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying statefulset scale up/down went fine on sts1 and sts2")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, false, true)
		// Scale down replicas of statefulset2 and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, false, true)

		// Scaling up statefulset sts1
		replicas1 += 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			replicas1, true, false)

		// Scaling down statefulset sts2
		replicas2 -= 2
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, true, false)

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
		pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, claim := range pvcs.Items {
			pv := getPvFromClaim(client, namespace, claim.Name)
			err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
			err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
					"kubernetes", volumeHandle))
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(false)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Operation Storm:
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.  Create 50 stateful sets with 1 replica and 50 with 2 replicas
		3.  Create 50 PVCs
		4.  Bring down secondary site
		5.  Scale up first 50 stateful sets to 3 replicas and scale down second 50 stateful sets to 0 replica
		6.  Delete 50 PVCs created in step 3
		7.  Create 50 new PVCs
		8.  Wait for secondary site VMs to come up and k8s to be healthy
		9.  Verify all stateful sets have scaled up/down successfully
		10. Scale down first 50 sts to 2 replicas
		11. Scale up second 50 statefulsets to 1 replica
		12. Verify all stateful sets have scaled up/down successfully
		13. Delete all stateful sets
		14. Delete all PVCs
		15. Bring secondary site up and wait for testbed to be normal
	*/
	ginkgo.It("[distributed] Operation Storm", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"
		var pvclaims []*v1.PersistentVolumeClaim
		var pvcList []*v1.PersistentVolumeClaim

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		var stsList []*appsv1.StatefulSet
		var replicas1, replicas2 int32
		prefix1 := "storm1-sts-"
		prefix2 := "storm2-sts-"
		framework.Logf("Create %d statefulsets with prefix %s", operationStormScale, prefix1)
		for i := 0; i < operationStormScale; i++ {
			statefulsetName := prefix1 + strconv.Itoa(i)
			framework.Logf("Creating statefulset: %s", statefulsetName)
			statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 1, statefulsetName)
			replicas1 = *(statefulset.Spec.Replicas)
			stsList = append(stsList, statefulset)
		}

		framework.Logf("Create %d statefulsets with prefix %s", operationStormScale, prefix2)
		for i := 0; i < operationStormScale; i++ {
			statefulsetName := prefix2 + strconv.Itoa(i)
			framework.Logf("Creating statefulset: %s", statefulsetName)
			statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false, true, 2, statefulsetName)
			replicas2 = *(statefulset.Spec.Replicas)
			stsList = append(stsList, statefulset)
		}

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		for i := 0; i < operationStormScale; i++ {
			framework.Logf("Creating pvc")
			pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
			pvclaims = append(pvclaims, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Wait for PVCs to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}
		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumes {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while scaling statfulsets and creation/deletion of PVCs")
		replicas1 += 2
		replicas2 -= 2
		var wg sync.WaitGroup
		ch := make(chan *v1.PersistentVolumeClaim)
		lock := &sync.Mutex{}
		wg.Add(5)
		go scaleStsReplicaInParallel(client, stsList, prefix1, replicas1, &wg)
		go scaleStsReplicaInParallel(client, stsList, prefix2, replicas2, &wg)
		go deletePvcInParallel(client, pvclaims, namespace, &wg)
		go createPvcInParallel(client, namespace, diskSize, sc, ch, lock, &wg, operationStormScale)
		go func() {
			for v := range ch {
				pvcList = append(pvcList, v)
			}
		}()
		framework.Logf("ch is %v", ch)
		go siteFailureInParallel(false, &wg)
		wg.Wait()
		close(ch)

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("pvcList is %v", pvcList)

		// Scale down replicas of statefulset2 and verify CNS entries for volumes

		// Waiting for pods status to be Ready and have scaled properly
		framework.Logf("Waiting for statefulset pod status with prefix %s to be Ready and have "+
			"scaled properly to replica %d", prefix1, replicas2)
		for _, statefulset := range stsList {
			if strings.Contains(statefulset.Name, prefix1) {
				scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
					replicas1, false, false)
			}
		}

		framework.Logf("Waiting for statefulset pod status with prefix %s to be Ready and have"+
			"scaled properly to replica %d", prefix2, replicas2)
		for _, statefulset := range stsList {
			if strings.Contains(statefulset.Name, prefix2) {
				fss.WaitForStatusReadyReplicas(client, statefulset, replicas2)
			}
		}

		ginkgo.By("Wait for PVCs to be in bound state")
		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(client, pvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		replicas1 -= 1
		replicas2 += 1

		framework.Logf("Scaling statefulset pod replicas with prefix %s to"+
			"%d number of replicas", prefix1, replicas1)
		for _, statefulset := range stsList {
			if strings.Contains(statefulset.Name, prefix1) {
				fss.UpdateReplicas(client, statefulset, replicas1)
			}
		}
		framework.Logf("Scaling statefulset pod replicas with prefix %s to"+
			"%d number of replicas", prefix1, replicas1)
		for _, statefulset := range stsList {
			if strings.Contains(statefulset.Name, prefix2) {
				fss.UpdateReplicas(client, statefulset, replicas2)
			}
		}

		framework.Logf("Waiting for statefulset pod status with prefix %s to be Ready and have"+
			"scaled properly to replica %d", prefix1, replicas1)
		for _, statefulset := range stsList {
			if strings.Contains(statefulset.Name, prefix1) {
				scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
					nil, replicas1, false, false)
			}
		}
		framework.Logf("Waiting for statefulset pod status with prefix %s to be Ready and have"+
			"scaled properly to replica %d", prefix2, replicas2)
		for _, statefulset := range stsList {
			if strings.Contains(statefulset.Name, prefix2) {
				scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
					replicas2, false, false)
			}
		}

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

		ginkgo.By("Bring up the secondary site")
		siteRestore(false)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
	/*
		Partial failure of secondary site
		Steps:
		1. Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2. Create a statefulset, deployment with volumes from the stretched datastore
		3. Bring down a esx server in the secondary site
		4. Verify that the VMs on the esx server which was brought down are started up on the
		   other esx servers in the secondary site
		5. Verify that the k8s cluster is healthy and all the k8s constructs created in step 2
		   are running and volume and application lifecycle actions work fine
		6. Restore secondary site back up and wait for testbed to be back to normal
		7. Delete all objects created in step 2 and 5
	*/
	ginkgo.It("[control-plane-on-primary] Partial failure of secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass for Statefulset")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-default"

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()

		ginkgo.By("Creating statefulset and deployment with volumes from the stretched datastore")
		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "")
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		replicas := *(statefulset.Spec.Replicas)
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Volume: %s should not be present in the CNS after it is deleted from "+
						"kubernetes", volumeHandle))
			}
		}()

		ginkgo.By("Bring down a host in secondary site")
		rand.New(rand.NewSource(time.Now().UnixNano()))
		max, min := 3, 0
		randomValue := rand.Intn(max-min) + min
		host := fds.secondarySiteHosts[randomValue]
		hostFailure(host, true)

		defer func() {
			ginkgo.By("Bring up host in secondary site")
			if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
				hostFailure(host, false)
				fds.hostsDown = nil
			}
		}()

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)
		// Scale down replicas of statefulset and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			ssPodsBeforeScaleDown, replicas-1, true, true)
		// Scale up replicas of statefulset and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas, true, true)

		ginkgo.By("Bring up host in secondary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			hostFailure(host, false)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

	})

	/*
		PV/PVC with Retain reclaim policy deletion while secondary site goes down
		and csi provisioner and csi-syncer leaders are in secondary site
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.  Ensure csi-provisioner and csi-syncer leaders are in secondary site
		3.  Create 30 PVCs and wait for each PVC binding with a PV
		4.	Delete PVCs created in step 3
		5.	Delete half of the PVs used by PVCs created in step 3
		6.  Bring down secondary site
		7.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
		8.  Verify PV and CNS volumes associated with PVC created in step 2 are also deleted successfully
		9.  Bring secondary site up and wait for testbed to be back to normal

	*/
	ginkgo.It("[distributed] PV/PVC with Retain reclaim policy deletion while secondary site goes down "+
		"and csi provisioner and csi-syncer leaders are in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-retain"
		var pvclaims, pvcs []*v1.PersistentVolumeClaim
		var pvs []*v1.PersistentVolume

		sc, err := createStorageClass(client, scParameters, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Ensuring %s leader is in secondary site", provisionerContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, provisionerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		framework.Logf("Ensuring %s leader is in secondary site", syncerContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, syncerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			framework.Logf("Creating pvc %v with reclaim policy Retain", i)
			pvc, err := createPVC(client, namespace, nil, diskSize, sc, "")
			pvclaims = append(pvclaims, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		defer func() {
			for _, claim := range pvclaims {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, pv := range persistentvolumes {
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By(fmt.Sprintf("Deleting FCD: %s", volumeHandle))
				err = e2eVSphere.deleteFCD(ctx, volumeHandle, defaultDatastore.Reference())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		for i := 0; i < volumeOpsScale/2; i++ {
			claim := pvclaims[i]
			pv := getPvFromClaim(client, namespace, claim.Name)
			pvs = append(pvs, pv)
			err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := volumeOpsScale / 2; i < volumeOpsScale; i++ {
			pvcs = append(pvcs, pvclaims[i])
		}

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while deleting pv")
		var wg sync.WaitGroup
		wg.Add(3)
		go deletePvcInParallel(client, pvcs, namespace, &wg)
		go deletePvInParallel(client, pvs, &wg)
		go siteFailureInParallel(false, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(false)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify all PVCs are deleted from namespace")
		for _, pvc := range pvclaims {
			err = waitForPvcToBeDeleted(ctx, client, pvc.Name, namespace)
		}

		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range pvs {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			framework.Logf("Persistent Volume %v still not deleted with err %v", pv.Name, err)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			// Orphan volumes may be left over here, hence logging those PVs and ignoring the error for now.
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			framework.Logf("Volume %v still not deleted from CNS with err %v", pv.Name, err)
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(false)
			fds.hostsDown = []string{}
		}

		siteRestore(true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		Static PV/PVC creation while secondary site goes down and csi-syncer leader is in secondary site
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.  Ensure csi-syncer leader is in secondary site
		3.  Create 30 fcds
		4.  Create a PV/PVC statically for the fcds created in step 3
		5.  Bring down secondary site
		6.  Verify that the VMs on the secondary site are started up on the other esx servers in the primary site
		7.  Wait for full sync
		8.  Verify that the PVCs created in step 3 is bound successfully
		9.	Verify CNS metadata for the volumes
		10. Bring secondary site up and wait for testbed to be back to normal
		11. Delete PVC created in step 4

	*/
	ginkgo.It("[distributed] Static PV/PVC creation while secondary site goes down"+
		" and csi-syncer leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storageThickPolicyName
		storageClassName = "nginx-sc-thick"
		var pvclaims []*v1.PersistentVolumeClaim
		//var persistentVolumes []*v1.PersistentVolume
		var fcdIDs []string

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Ensuring %s leader is in secondary site", syncerContainerName)
		err = changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, syncerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < volumeOpsScale; i++ {
			ginkgo.By("Creating FCD Disk")
			fcdID, err := e2eVSphere.createFCD(ctx, "FCD"+strconv.Itoa(i), diskSizeInMb, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fcdIDs = append(fcdIDs, fcdID)
		}

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCDs to sync with pandora",
			pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		// Get the list of csi pods running in CSI namespace
		csipods, err := client.CoreV1().Pods(csiNs).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bring down the secondary site while creating static pv and pvcs")
		var wg sync.WaitGroup
		ch := make(chan *v1.PersistentVolumeClaim)
		wg.Add(2)
		go createStaticPvAndPvcInParallel(client, ctx, fcdIDs, ch, namespace, &wg, volumeOpsScale)
		go func() {
			for v := range ch {
				pvclaims = append(pvclaims, v)
			}
		}()
		go siteFailureInParallel(false, &wg)
		wg.Wait()
		close(ch)

		defer func() {
			pvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, claim := range pvcs.Items {
				pv := getPvFromClaim(client, namespace, claim.Name)
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify it's PV and corresponding volumes are deleted from CNS")
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < volumeOpsScale; i++ {
			volHandle := persistentvolumes[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			framework.Logf("Verifying CNS entry is present in cache for pv: %s", persistentvolumes[i].Name)
			_, err = e2eVSphere.queryCNSVolumeWithResult(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pvclaim := range pvclaims {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Verify PVs, volumes are deleted from CNS")
		for _, pv := range persistentvolumes {
			err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
				pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volumeHandle := pv.Spec.CSI.VolumeHandle
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, fcdId := range fcdIDs {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdId))
			err := e2eVSphere.deleteFCD(ctx, fcdId, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the secondary site")
		siteRestore(false)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
			Site failover during full sync
			Steps:
			1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
			2.	Create 6 PVCs with reclaim policy Delete and 8 with reclaim policy Retain
				and wait for them to be bound
			3.	Delete four PVCs with reclaim policy Retain
			4.	Delete two PVs reclaim policy Retain related to PVC used in step 3
			5.	Create two pods using PVCs with reclaim policy Delete
			6.	Bring vsan-health service down
			7.	Create two pods with two PVCs each
			8.	Create two static PVs with disk left after step 4
			9.	Create two PVCs to bind to PVs with reclaim policy Retain
			10. Delete four PVCs with reclaim policy Retain different from the ones used in step 3 and 9
			11. Delete two PVs reclaim policy Retain related to PVC used in step 10
			12. Add labels to all PVs, PVCs
			13. Bring vsan-health service up when full sync is triggered
			14. Bring down primary site
			15. Verify that the VMs on the primary site are started up on the other esx servers
		        in the secondary site
			16. Wait for full sync
			17. Verify CNS entries
			18. Delete all pods, PVCs and PVs
			19. Bring primary site up and wait for testbed to be back to normal

	*/
	ginkgo.It("[primary-centric][distributed] Primary site failover during full sync when syncer"+
		" pod leader is in primary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-delete"
		var pods []*v1.Pod
		var pvclaimsWithDelete, pvclaimsWithRetain []*v1.PersistentVolumeClaim
		var volHandles []string

		framework.Logf("Ensuring %s leader is in primary site", syncerContainerName)
		err := changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, syncerContainerName, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scRetain, err := createStorageClass(client, scParameters, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		scDelete, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, scRetain.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.StorageV1().StorageClasses().Delete(ctx, scDelete.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < 6; i++ {
			framework.Logf("Creating pvc %v with reclaim policy Delete", i)
			pvc, err := createPVC(client, namespace, nil, diskSize, scDelete, "")
			pvclaimsWithDelete = append(pvclaimsWithDelete, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < 8; i++ {
			framework.Logf("Creating pvc %v with reclaim policy Retain", i)
			pvc, err := createPVC(client, namespace, nil, diskSize, scRetain, "")
			pvclaimsWithRetain = append(pvclaimsWithRetain, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumesRetain, err := fpv.WaitForPVClaimBoundPhase(client, pvclaimsWithRetain,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < 8; i++ {
			volHandle := persistentvolumesRetain[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		persistentvolumesDelete, err := fpv.WaitForPVClaimBoundPhase(client, pvclaimsWithDelete,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < 6; i++ {
			volHandle := persistentvolumesDelete[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		defer func() {
			for _, claim := range pvclaimsWithDelete {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, claim := range pvclaimsWithRetain {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumesDelete {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, pv := range persistentvolumesRetain {
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		for i := 0; i < 4; i++ {
			if i == 0 || i == 1 {
				pv := getPvFromClaim(client, namespace, pvclaimsWithRetain[i].Name)
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Deleting pv %s from pvc: %s", pv.Name, pvclaimsWithRetain[i].Name)
				volHandle := pv.Spec.CSI.VolumeHandle
				volHandles = append(volHandles, volHandle)
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for i := 0; i < 2; i++ {
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaimsWithDelete[i]}, false, execCommand)
			framework.Logf("Created pod %s", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		}
		framework.Logf("Stopping vsan-health on the vCenter host")
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanHealthServiceStopped = true

		for i := 2; i < 4; i++ {
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaimsWithDelete[i]}, false, execCommand)
			framework.Logf("Created pod %s", pod.Name)
			gomega.Expect(err).To(gomega.HaveOccurred())
			pods = append(pods, pod)
		}

		// Creating label for PV.
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)
		var staticPvcs []*v1.PersistentVolumeClaim
		var staticPvs []*v1.PersistentVolume
		for i := 0; i < 2; i++ {
			staticPVLabels["fcd-id"] = volHandles[i]

			ginkgo.By("Creating static PV")
			pv := getPersistentVolumeSpec(volHandles[i], v1.PersistentVolumeReclaimDelete, staticPVLabels, ext4FSType)
			pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			staticPvs = append(staticPvs, pv)

			ginkgo.By("Creating PVC from static PV")
			pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			staticPvcs = append(staticPvcs, pvc)

			framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv, pvc))

		}

		defer func() {
			ginkgo.By("Deleting static pvcs and pvs")
			for _, pvc := range staticPvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, pv := range staticPvs {
				err := fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		for i := 4; i < 8; i++ {
			if i == 4 || i == 5 {
				pv := getPvFromClaim(client, namespace, pvclaimsWithRetain[i].Name)
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			} else {
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pvc := range allPvcs.Items {
			framework.Logf("Updating labels %+v for pvc %s in namespace %s",
				labels, pvc.Name, namespace)
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc.Labels = labels
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Error on updating pvc labels is: %v", err)
		}

		allPvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pv := range allPvs.Items {
			framework.Logf("Updating labels %+v for pv %s in namespace %s",
				labels, pv.Name, namespace)
			pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv.Labels = labels
			_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Error on updating pv labels is: %v", err)
		}

		framework.Logf("Starting vsan-health on the vCenter host")
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		framework.Logf("Sleeping full-sync interval for vsan health service " +
			"to be fully up")
		time.Sleep(time.Duration(60) * time.Second)

		csipods, err := client.CoreV1().Pods(csiSystemNamespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Get restConfig.
		restConfig := getRestConfigClient()
		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		enableFullSyncTriggerFss(ctx, client, csiSystemNamespace, fullSyncFss)
		ginkgo.By("Bring down the primary site while full sync is going on")
		var wg sync.WaitGroup
		wg.Add(2)
		go triggerFullSyncInParallel(ctx, client, cnsOperatorClient, &wg)
		go siteFailureInParallel(true, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the primary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(true)
				fds.hostsDown = []string{}
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Trigger 2 full syncs as full sync might be interrupted during site failover")
		triggerFullSync(ctx, client, cnsOperatorClient)

		ginkgo.By("Checking whether pods are in Running state")
		for _, pod := range pods {
			framework.Logf("Pod is %s", pod.Name)
			err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pvc := range allPvcs.Items {
			ginkgo.By(fmt.Sprintf("Verifying labels %+v are updated for pvc %s in namespace %s",
				labels, pvc.Name, namespace))
			pv := getPvFromClaim(client, namespace, pvc.Name)
			err = e2eVSphere.verifyLabelsAreUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pv := range allPvs.Items {
			ginkgo.By(fmt.Sprintf("Verifying labels %+v are updated for pv %s",
				labels, pv.Name))
			err = e2eVSphere.verifyLabelsAreUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Deleting all the pods in the namespace
		for _, pod := range pods {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(true)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Secondary site failover during full sync when syncer pod leader is in secondary site
		Steps:
		1.  Configure a vanilla multi-master K8s cluster with inter and intra site replication
		2.	Ensure syncer leader is in secondary site
		3.	Create 6 PVCs with reclaim policy Delete and 8 with reclaim policy Retain
			and wait for them to be bound
		4.	Delete four PVCs with reclaim policy Retain
		5.	Delete two PVs reclaim policy Retain related to PVC used in step 3
		6.	Create two pods using PVCs with reclaim policy Delete
		7.	Bring vsan-health service down
		8.	Create two pods with two PVCs each
		9.	Create two static PVs with disk left after step 4
		10.	Create two PVCs to bind to PVs with reclaim policy Retain
		11. Delete four PVCs with reclaim policy Retain different from the ones used in step 3 and 9
		12. Delete two PVs reclaim policy Retain related to PVC used in step 10
		13. Add labels to all PVs, PVCs
		14. Bring vsan-health service up when full sync is triggered
		15. Bring down secondary site
		16. Verify that the VMs on the secondary site are started up on the other esx servers
			in the primary site
		17. Wait for full sync
		18. Verify CNS entries
		19. Delete all pods, PVCs and PVs
		19. Bring secondary site up and wait for testbed to be back to normal

	*/
	ginkgo.It("[distributed] Secondary site failover during full sync when syncer"+
		" pod leader is in secondary site", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Creating StorageClass")
		// decide which test setup is available to run
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		scParameters = map[string]string{}
		scParameters["StoragePolicyName"] = storagePolicyName
		storageClassName = "nginx-sc-delete"
		var pods []*v1.Pod
		var pvclaimsWithDelete, pvclaimsWithRetain []*v1.PersistentVolumeClaim
		var volHandles []string

		framework.Logf("Ensuring %s leader is in secondary site", syncerContainerName)
		err := changeLeaderOfContainerToComeUpOnMaster(ctx, client, sshClientConfig, syncerContainerName, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scRetain, err := createStorageClass(client, scParameters, nil, v1.PersistentVolumeReclaimRetain, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
		scDelete, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, scRetain.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.StorageV1().StorageClasses().Delete(ctx, scDelete.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		for i := 0; i < 6; i++ {
			framework.Logf("Creating pvc %v with reclaim policy Delete", i)
			pvc, err := createPVC(client, namespace, nil, diskSize, scDelete, "")
			pvclaimsWithDelete = append(pvclaimsWithDelete, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < 8; i++ {
			framework.Logf("Creating pvc %v with reclaim policy Retain", i)
			pvc, err := createPVC(client, namespace, nil, diskSize, scRetain, "")
			pvclaimsWithRetain = append(pvclaimsWithRetain, pvc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		persistentvolumesRetain, err := fpv.WaitForPVClaimBoundPhase(client, pvclaimsWithRetain,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < 8; i++ {
			volHandle := persistentvolumesRetain[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		persistentvolumesDelete, err := fpv.WaitForPVClaimBoundPhase(client, pvclaimsWithDelete,
			framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < 6; i++ {
			volHandle := persistentvolumesDelete[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		}

		defer func() {
			for _, claim := range pvclaimsWithDelete {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, claim := range pvclaimsWithRetain {
				err := fpv.DeletePersistentVolumeClaim(client, claim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Verify PVs, volumes are deleted from CNS")
			for _, pv := range persistentvolumesDelete {
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, pv := range persistentvolumesRetain {
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err := fpv.WaitForPersistentVolumeDeleted(client, pv.Name, framework.Poll,
					framework.PodDeleteTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		for i := 0; i < 4; i++ {
			if i == 0 || i == 1 {
				pv := getPvFromClaim(client, namespace, pvclaimsWithRetain[i].Name)
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Deleting pv %s from pvc: %s", pv.Name, pvclaimsWithRetain[i].Name)
				volHandle := pv.Spec.CSI.VolumeHandle
				volHandles = append(volHandles, volHandle)
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for i := 0; i < 2; i++ {
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaimsWithDelete[i]}, false, execCommand)
			framework.Logf("Created pod %s", pod.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pods = append(pods, pod)
		}
		framework.Logf("Stopping vsan-health on the vCenter host")
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		err = invokeVCenterServiceControl(stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(vsanhealthServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isVsanHealthServiceStopped = true

		for i := 2; i < 4; i++ {
			pod, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaimsWithDelete[i]}, false, execCommand)
			framework.Logf("Created pod %s", pod.Name)
			pods = append(pods, pod)
			gomega.Expect(err).To(gomega.HaveOccurred())
		}

		// Creating label for PV.
		// PVC will use this label as Selector to find PV
		staticPVLabels := make(map[string]string)
		var staticPvcs []*v1.PersistentVolumeClaim
		var staticPvs []*v1.PersistentVolume

		for i := 0; i < 2; i++ {
			staticPVLabels["fcd-id"] = volHandles[i]

			ginkgo.By("Creating static PV")
			pv := getPersistentVolumeSpec(volHandles[i], v1.PersistentVolumeReclaimDelete, staticPVLabels, ext4FSType)
			pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			staticPvs = append(staticPvs, pv)

			ginkgo.By("Creating PVC from static PV")
			pvc := getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
			pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			staticPvcs = append(staticPvcs, pvc)

			framework.ExpectNoError(fpv.WaitOnPVandPVC(client, framework.NewTimeoutContextWithDefaults(), namespace, pv, pvc))

		}

		defer func() {
			ginkgo.By("Deleting static pvcs and pvs")
			for _, pvc := range staticPvcs {
				err := fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			for _, pv := range staticPvs {
				err := fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.WaitForPersistentVolumeDeleted(client, pv.Name, poll,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				volumeHandle := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		for i := 4; i < 8; i++ {
			if i == 4 || i == 5 {
				pv := getPvFromClaim(client, namespace, pvclaimsWithRetain[i].Name)
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = fpv.DeletePersistentVolume(client, pv.Name)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				framework.Logf("Deleting pvc %v with reclaim policy Retain", pvclaimsWithRetain[i].Name)
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimsWithRetain[i].Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		labels := make(map[string]string)
		labels[labelKey] = labelValue

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pvc := range allPvcs.Items {
			framework.Logf("Updating labels %+v for pvc %s in namespace %s",
				labels, pvc.Name, namespace)
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvc.Labels = labels
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Error on updating pvc labels is: %v", err)
		}

		allPvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pv := range allPvs.Items {
			framework.Logf("Updating labels %+v for pv %s in namespace %s",
				labels, pv.Name, namespace)
			pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv.Labels = labels
			_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"Error on updating pv labels is: %v", err)
		}

		framework.Logf("Starting vsan-health on the vCenter host")
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		framework.Logf("Sleeping full-sync interval for vsan health service " +
			"to be fully up")
		time.Sleep(time.Duration(60) * time.Second)

		csipods, err := client.CoreV1().Pods(csiSystemNamespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Get restConfig.
		restConfig := getRestConfigClient()
		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		enableFullSyncTriggerFss(ctx, client, csiSystemNamespace, fullSyncFss)
		ginkgo.By("Bring down the secondary site while full sync is going on")
		var wg sync.WaitGroup
		wg.Add(2)
		go triggerFullSyncInParallel(ctx, client, cnsOperatorClient, &wg)
		go siteFailureInParallel(false, &wg)
		wg.Wait()

		defer func() {
			ginkgo.By("Bring up the secondary site before terminating the test")
			if len(fds.hostsDown) > 0 {
				siteRestore(false)
				fds.hostsDown = nil
			}
		}()

		ginkgo.By("Wait for k8s cluster to be healthy")
		wait4AllK8sNodesToBeUp(ctx, client, nodeList)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Trigger 2 full syncs as full sync might be interrupted during site failover")
		triggerFullSync(ctx, client, cnsOperatorClient)

		ginkgo.By("Checking whether pods are in Running state")
		for _, pod := range pods {
			framework.Logf("Pod is %s", pod.Name)
			err = waitForPodsToBeInErrorOrRunning(client, pod.Name, namespace, pollTimeout*4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for _, pvc := range allPvcs.Items {
			ginkgo.By(fmt.Sprintf("Verifying labels %+v are updated for pvc %s in namespace %s",
				labels, pvc.Name, namespace))
			pv := getPvFromClaim(client, namespace, pvc.Name)
			err = e2eVSphere.verifyLabelsAreUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePVC), pvc.Name, pvc.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		for _, pv := range allPvs.Items {
			ginkgo.By(fmt.Sprintf("Verifying labels %+v are updated for pv %s",
				labels, pv.Name))
			err = e2eVSphere.verifyLabelsAreUpdated(pv.Spec.CSI.VolumeHandle, labels,
				string(cnstypes.CnsKubernetesEntityTypePV), pv.Name, pv.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		// Deleting all pods in namespace
		for _, pod := range pods {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 {
			siteRestore(false)
			fds.hostsDown = []string{}
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})
