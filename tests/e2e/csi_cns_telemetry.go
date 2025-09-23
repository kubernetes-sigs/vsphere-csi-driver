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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var _ = ginkgo.Describe("[csi-block-vanilla] [csi-file-vanilla] [csi-block-vanilla-serialized] "+
	"CNS-CSI Cluster Distribution Telemetry", func() {

	f := framework.NewDefaultFramework("csi-cns-telemetry")
	var (
		client       clientset.Interface
		namespace    string
		scParameters map[string]string
		datastoreURL string
		csiNamespace string
		csiReplicas  int32
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas
		// Reset the cluster distribution value to default value "CSI-Vanilla".
		setClusterDistribution(ctx, client, vanillaClusterDistribution)
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		collectPodLogs(ctx, client, csiSystemNamespace)
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Reset the cluster distribution value to default value "CSI-Vanilla".
		setClusterDistribution(ctx, client, vanillaClusterDistribution)
		collectPodLogs(ctx, client, csiSystemNamespace)
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	// Test to verify cluster distribution set in PVC is being honored during
	// volume creation.
	// Steps
	// 1. Create StorageClass.
	// 2. Create PVC with valid disk size.
	// 3. Expect PVC to pass and cluster distribution value set.
	// 4. Update cluster-distribution value.
	// 5. Create another PVC with valid disk size.
	// 6. Expect PVC to pass and cluster distribution value set to latest update.
	// 7. Expect the old PVC to reflect the latest cluster-distribution value.

	// Test for cluster-distribution value presence.
	ginkgo.It("[pq-vanilla-file]Verify dynamic provisioning of pvc has cluster-distribution value "+
		"updated", ginkgo.Label(p0, block, file, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var storageclasspvc1 *storagev1.StorageClass
		var pvclaim1 *v1.PersistentVolumeClaim
		var storageclasspvc2 *storagev1.StorageClass
		var pvclaim2 *v1.PersistentVolumeClaim
		var fullSyncWaitTime int
		var err error

		// Read full-sync value.
		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			framework.Logf("Full-Sync interval time value is = %v", fullSyncWaitTime)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating a PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclasspvc1, pvclaim1, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx,
				storageclasspvc1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes1, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim1}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		volHandle1 := persistentvolumes1[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle1).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim1.Name, pvclaim1.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC1 with VolumeID: %s", volHandle1))
		queryResult1, err := e2eVSphere.queryCNSVolumeWithResult(volHandle1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult1.Volumes) > 0).To(gomega.BeTrue())

		framework.Logf("Cluster-distribution value on CNS is %s",
			queryResult1.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution)
		gomega.Expect(queryResult1.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution).Should(
			gomega.Equal(vanillaClusterDistribution), "Wrong/empty cluster-distribution name present on CNS")

		ginkgo.By("Setting the cluster-distribution value to empty")
		setClusterDistribution(ctx, client, "")
		ginkgo.By("Restart CSI driver")
		collectPodLogs(ctx, client, csiSystemNamespace)
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Reset the cluster distribution value to default value "CSI-Vanilla".
			setClusterDistribution(ctx, client, vanillaClusterDistribution)
		}()

		ginkgo.By("Creating Second PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclasspvc2, pvclaim2, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx,
				storageclasspvc2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		persistentvolumes2, err := fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim2}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision Second volume")
		volHandle2 := persistentvolumes2[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim2.Name, pvclaim2.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		framework.Logf("Force trigger full sync for pvc metadata to reflect new changes")
		// Get restConfig.
		restConfig := getRestConfigClient()
		cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		enableFullSyncTriggerFss(ctx, client, csiSystemNamespace, fullSyncFss)
		triggerFullSync(ctx, cnsOperatorClient)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC2 with VolumeID: %s", volHandle2))
		queryResult2, err := e2eVSphere.queryCNSVolumeWithResult(volHandle2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())

		ginkgo.By("Verifying cluster-distribution value specified in secret is honored")
		framework.Logf("Cluster-distribution value on CNS is %s",
			queryResult2.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution)
		gomega.Expect(queryResult2.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution).Should(
			gomega.Equal("VanillaK8S"), "Wrong/empty cluster-distribution name present on CNS")

		// For Old PVCs to reflect the latest Cluster-Distribution Value, wait for
		// full sync.
		framework.Logf("Sleeping full-sync interval for all the volumes Metadata " +
			"to reflect the cluster-distribution value to = Empty")
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		// Add additional safe wait time for cluster-distribution to reflect on
		// metadata.
		framework.Logf("Sleeping for additional safe one min for volumes Metadata " +
			"to reflect the cluster-distribution value to = Empty")
		time.Sleep(time.Duration(pollTimeoutShort))

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC1 with VolumeID: %s", volHandle1))
		queryResult3, err := e2eVSphere.queryCNSVolumeWithResult(volHandle1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult3.Volumes) > 0).To(gomega.BeTrue())

		framework.Logf("Cluster-distribution value on CNS is %s",
			queryResult3.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution)
		gomega.Expect(queryResult3.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution).Should(
			gomega.Equal("VanillaK8S"), "Wrong/empty cluster-distribution name present on CNS")

		ginkgo.By("Setting the cluster-distribution value with escape character and special characters")
		setClusterDistribution(ctx, client, vanillaClusterDistributionWithSpecialChar)
		collectPodLogs(ctx, client, csiSystemNamespace)
		restartSuccess, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// For Old PVCs to reflect the latest Cluster-Distribution Value, wait for
		// full sync.
		framework.Logf("Sleeping full-sync interval for all the volumes Metadata to reflect "+
			"the cluster-distribution value to = %s", vanillaClusterDistributionWithSpecialChar)
		time.Sleep(time.Duration(fullSyncWaitTime) * time.Second)

		// Add additional safe wait time for cluster-distribution to reflect on
		// metadata.
		framework.Logf("Sleeping for additional safe one min for volumes Metadata to reflect "+
			"the cluster-distribution value to = %s", vanillaClusterDistributionWithSpecialChar)
		time.Sleep(time.Duration(pollTimeoutShort))

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult on PVC2 with VolumeID: %s", volHandle2))
		queryResult2, err = e2eVSphere.queryCNSVolumeWithResult(volHandle2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult2.Volumes) > 0).To(gomega.BeTrue())
		framework.Logf("Cluster-distribution value on CNS is %s",
			queryResult2.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution)

		queryResult3, err = e2eVSphere.queryCNSVolumeWithResult(volHandle1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult3.Volumes) > 0).To(gomega.BeTrue())
		framework.Logf("Cluster-distribution value on CNS is %s",
			queryResult3.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution)

		ginkgo.By("Verifying cluster-distribution value specified in secret is honored")
		gomega.Expect(queryResult2.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution).Should(
			gomega.Equal(vanillaClusterDistributionWithSpecialChar),
			"Wrong/empty cluster-distribution name present on CNS")
		gomega.Expect(queryResult3.Volumes[0].Metadata.ContainerClusterArray[0].ClusterDistribution).Should(
			gomega.Equal(vanillaClusterDistributionWithSpecialChar),
			"Wrong/empty cluster-distribution name present on CNS")
	})

	// Test to verify PVC goes to Pending when cluster-distribution name has more than 128 characters
	// Steps
	// 1. Update cluster-distribution value to  more than 128 characters
	// 2. Create a PVC.
	// 3. Expect PVC to go to Pending state.

	ginkgo.It("[pq-vanilla-file]Verify PVC goes to Pending when cluster distribution name has more"+
		" than 128 characters", ginkgo.Label(p0, block, file, vanilla, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvclaim *v1.PersistentVolumeClaim
		var storageclasspvc *storagev1.StorageClass
		var err error

		ginkgo.By("Setting the cluster-distribution name having more than 128 characters")
		clsDistributionName, err := genrateRandomString(130)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		setClusterDistribution(ctx, client, clsDistributionName)
		ginkgo.By("Restart CSI driver")
		collectPodLogs(ctx, client, csiSystemNamespace)
		restartSuccess, err := restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(restartSuccess).To(gomega.BeTrue(), "csi driver restart not successful")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Reset the cluster distribution value to default value "CSI-Vanilla".
			setClusterDistribution(ctx, client, vanillaClusterDistribution)
		}()

		ginkgo.By("Creating a PVC")
		scParameters[scParamDatastoreURL] = datastoreURL
		storageclasspvc, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err := client.StorageV1().StorageClasses().Delete(ctx,
				storageclasspvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to fail")
		err = fpv.WaitForPersistentVolumeClaimPhase(ctx,
			v1.ClaimPending, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Second*130)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		expectedErrMsg := "A specified parameter was not correct: createSpecs.metadata.containerCluster.clusterDistribution"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		waitErr := wait.PollUntilContextTimeout(ctx, poll*5, pollTimeoutSixMin, true,
			func(ctx context.Context) (bool, error) {
				errorOccurred := checkEventsforError(client, pvclaim.Namespace,
					metav1.ListOptions{FieldSelector: fmt.Sprintf("involvedObject.name=%s", pvclaim.Name)},
					expectedErrMsg)
				return errorOccurred, nil
			})
		gomega.Expect(waitErr).ToNot(gomega.HaveOccurred())
	})
})
