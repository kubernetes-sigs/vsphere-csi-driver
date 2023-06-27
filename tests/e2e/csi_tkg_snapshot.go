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
	"fmt"
	"os"
	"strconv"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
)

var _ = ginkgo.Describe("[tkg-snapshot] Volume Snapshot Basic Test", func() {
	f := framework.NewDefaultFramework("volume-snapshot")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                 clientset.Interface
		namespace              string
		scParameters           map[string]string
		datastoreURL           string
		pandoraSyncWaitTime    int
		restConfig             *restclient.Config
		guestClusterRestConfig *restclient.Config
		snapc                  *snapclient.Clientset
		storagePolicyName      string
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//Get snapshot client using the rest config
		if !guestCluster {
			restConfig = getRestConfigClient()
			snapc, err = snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			guestClusterRestConfig = getRestConfigClientForGuestCluster(guestClusterRestConfig)
			snapc, err = snapclient.NewForConfig(guestClusterRestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
	})

	ginkgo.AfterEach(func() {
		if guestCluster {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			framework.Logf("Collecting supervisor PVC events before performing PV/PVC cleanup")
			eventList, err := svcClient.CoreV1().Events(svcNamespace).List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, item := range eventList.Items {
				framework.Logf(fmt.Sprintf(item.Message))
			}
		}
	})

	/* Create/Delete snapshot via k8s API using VolumeSnapshotContent (Pre-Provisioned Snapshots)

	//Steps
	1. In this approach create a dynamic VolumeSnapshot in Guest with a VolumeSnapshotClass with “Delete”
	deletion policy.
	2. Note the VolumeSnapshot name created on the Supervisor.
	3. Explicitly change the deletionPolicy of VolumeSnapshotContent on Guest to “Retain”.
	4. Delete the VolumeSnapshot. This will leave the VolumeSnapshotContent on the Guest as is,
	since deletionPolicy was “Retain”
	5. Explicitly delete the VolumeSnapshotContent.
	6. In this approach, we now have Supervisor VolumeSnapshot that doesn’t have a corresponding
	VolumeSnapshot-VolumeSnapshotContent on Guest.
	7. Create a VolumeSnapshotContent that points to the Supervisor VolumeSnapshot, and create a
	VolumeSnapshot on Guest that point to the VolumeSnapshotContent.
	*/
	ginkgo.It("[tkg-snapshot] TC2Create and Delete snapshot using Pre-Provisioned Snapshots", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var volHandle string
		var pvclaims []*v1.PersistentVolumeClaim

		if vanillaCluster {
			scParameters[scParamDatastoreURL] = datastoreURL
		} else if guestCluster {
			scParameters[svStorageClassName] = storagePolicyName
		}

		ginkgo.By("Create storage class and PVC")
		storageclass, pvclaim, err := createPVCAndStorageClass(client,
			namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		if guestCluster {
			volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		}
		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Verify using CNS Query API if VolumeID retrieved from PV is present.
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
		gomega.Expect(queryResult.Volumes[0].VolumeId.Id).To(gomega.Equal(volHandle))

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if vanillaCluster {
				err = snapc.SnapshotV1().VolumeSnapshotClasses().Delete(ctx, volumeSnapshotClass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create a dyanmic volume snapshot")
		volumeSnapshot, snapshotContent, snapshotCreated,
			snapshotContentCreated, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
			pvclaim, diskSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if snapshotCreated {
				framework.Logf("Deleting volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if snapshotContentCreated {
				framework.Logf("Deleting volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		framework.Logf("Get volume snapshot handle from Supervisor Cluster")
		snapshotId, _, svcVolumeSnapshotName, err := getSnapshotHandleFromSupervisorCluster(ctx, volumeSnapshotClass,
			*snapshotContent.Status.SnapshotHandle)

		ginkgo.By("Create Preprovisioned snapshot")
		_, staticSnapshot, err := createPreProvisionedSnapshotForGuestCluster(ctx, volumeSnapshot, snapshotContent,
			snapc, namespace, pandoraSyncWaitTime, svcVolumeSnapshotName)

		ginkgo.By("Delete Preprovisioned snapshot")
		staticSnapshotCreated, StaticSnapshotContentCreated, err := deleteDynamicVolumeSnapshot(ctx, snapc, namespace,
			staticSnapshot, pandoraSyncWaitTime, volHandle, snapshotId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if staticSnapshotCreated {
				framework.Logf("Deleting static volume snapshot")
				deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if StaticSnapshotContentCreated {
				framework.Logf("Deleting static volume snapshot content")
				deleteVolumeSnapshotContentWithPandoraWait(ctx, snapc,
					*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)

				framework.Logf("Wait till the volume snapshot is deleted")
				err = waitForVolumeSnapshotContentToBeDeleted(*snapc, ctx, *volumeSnapshot.Status.BoundVolumeSnapshotContentName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

	})

})
