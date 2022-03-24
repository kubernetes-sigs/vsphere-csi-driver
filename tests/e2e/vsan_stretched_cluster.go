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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"golang.org/x/crypto/ssh"
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
)

var _ = ginkgo.Describe("[vsan-stretch-vanilla] vsan stretched cluster tests", func() {
	f := framework.NewDefaultFramework("vsan-stretch")
	const defaultVolumeOpsScale = 30
	var (
		client                 clientset.Interface
		namespace              string
		nodeList               *v1.NodeList
		storagePolicyName      string
		scParameters           map[string]string
		storageClassName       string
		csiNs                  string
		fullSyncWaitTime       int
		volumeOpsScale         int
		storageThickPolicyName string
		labelKey               string
		labelValue             string
		sshClientConfig        *ssh.ClientConfig
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

		csiNs = GetAndExpectStringEnvVar(envCSINamespace)

		initialiseFdsVar(ctx)
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
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
	ginkgo.It("Primary site down", func() {
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
		statefulset, _, volumesBeforeScaleDown := createStsDeployment(ctx, client, namespace, sc, true,
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
			volumesBeforeScaleDown, replicas, true, true)

		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Statefulset scale up/down while primary site goes down", func() {
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
		statefulset1, _, volumes1BeforeScaleDown := createStsDeployment(ctx, client, namespace, sc, false, true, 1, "web")
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying statefulset scale up/down went fine on sts1 and sts2")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			volumes1BeforeScaleDown, replicas1, false, true)
		// Scale down replicas of statefulset2 and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, false, true)

		// Scaling up statefulset sts1
		replicas1 += 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			volumes1BeforeScaleDown, replicas1, true, false)

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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Pod deletion while primary site goes down", func() {
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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

			err = verifyVolumeMetadataInCNS(&e2eVSphere, volHandle,
				pvclaims[i].Name, persistentvolumes[i].Name, pods[i].Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Bring up the primary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(true)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("PVC creation while primary site goes down", func() {
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Primary site network isolation", func() {
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
		statefulset, deployment, volumesBeforeScaleDown := createStsDeployment(ctx, client, namespace, sc, true,
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
			volumesBeforeScaleDown, replicas, true, true)

		ginkgo.By("Bring up the primary site")
		siteNetworkFailure(true, true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("PVC deletion while primary site goes down", func() {
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Pod creation while primary site goes down", func() {
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Label updates to PV, PVC, pod while primary site goes down", func() {
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
		siteRestore(true)

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("PVC creation while secondary site goes down and csi provisioner leader is in secondary site", func() {
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Secondary site down", func() {
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
		statefulset, _, volumesBeforeScaleDown := createStsDeployment(ctx, client, namespace, sc, true,
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
			volumesBeforeScaleDown, replicas, true, true)

		ginkgo.By("Bring up the secondary site")
		if len(fds.hostsDown) > 0 && fds.hostsDown != nil {
			siteRestore(false)
			fds.hostsDown = nil
		}

		ginkgo.By("Wait for k8s cluster to be healthy")
		// wait for the VMs to move back
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Network failure between sites", func() {
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
		statefulset1, _, volumes1BeforeScaleDown := createStsDeployment(ctx, client, namespace, sc, false, true, 1, "web")
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Check if csi pods are running fine after site failure
		err = fpod.WaitForPodsRunningReady(client, csiNs, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying statefulset scale up/down went fine on sts1 and sts2")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			volumes1BeforeScaleDown, replicas1, false, true)
		// Scale down replicas of statefulset2 and verify CNS entries for volumes
		scaleDownStsAndVerifyPodMetadata(ctx, client, namespace, statefulset2,
			ss2PodsBeforeScaleDown, replicas2, false, true)

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)

		// Scaling up statefulset sts1
		replicas1 += 2
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset1,
			volumes1BeforeScaleDown, replicas1, true, false)

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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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
	ginkgo.It("Witness failure", func() {
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
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

		statefulset, _, volumesBeforeScaleDown := createStsDeployment(ctx, client, namespace, sc, true, false, 0, "")
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
		err = waitForAllNodes2BeReady(ctx, client, pollTimeout*4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying volume lifecycle actions works fine")
		volumeLifecycleActions(ctx, client, namespace, sc)

		ginkgo.By(fmt.Sprintf("Scaling up statefulset: %v to number of Replica: %v",
			statefulset.Name, replicas))
		// Scale up replicas of statefulset and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			volumesBeforeScaleDown, replicas, true, true)

		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)
	})
})
