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

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

// Test performs following operations.
//
// Steps:
// 1. Create a storage class.
// 2. Create nginx service.
// 3. Create nginx statefulsets with 3 replicas.
// 4. Wait until all Pods are ready and PVCs are bounded with PV.
// 5. Expect PVC to pass and cluster distribution value set to latest update.
// 6. Scaledown nginx statefulsets with 0 replicas.
// 7. Delete all PVCs from the tests namespace.
// 8. Delete the storage class.

var _ = ginkgo.Describe("[csi-block-vanilla] [csi-file-vanilla] [csi-supervisor] [csi-guest] "+
	"[csi-block-vanilla-parallelized] CNS-CSI Cluster Distribution for StatefulSets", func() {
	f := framework.NewDefaultFramework("csi-cns-telemetry")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace         string
		client            clientset.Interface
		storagePolicyName string
		scParameters      map[string]string
		storageClassName  string
	)
	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))).
				NotTo(gomega.HaveOccurred())
		}
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		if vanillaCluster {
			// Reset the cluster distribution value to default value "CSI-Vanilla".
			setClusterDistribution(ctx, client, vanillaClusterDistribution)
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(ctx, client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		if vanillaCluster {
			// Reset the cluster distribution value to default value "CSI-Vanilla".
			setClusterDistribution(ctx, client, vanillaClusterDistribution)
		} else if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		} else {
			svcClient, svNamespace := getSvcClientAndNamespace()
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}
	})

	ginkgo.It("[ef-vks][ef-vks-n1][ef-vks-n2] [pq-vanilla-file]Statefulset service for cluster-distribution metadata "+
		"check", ginkgo.Label(p0, block, file, vanilla, wcp, tkg, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		var clusterDistributionValue string
		var sc *v1.StorageClass
		var err error
		clusterFlavor := GetAndExpectStringEnvVar(envClusterFlavor)
		defer cancel()
		// Decide which test setup is available to run.
		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			scParameters = nil
			clusterDistributionValue = vanillaClusterDistribution
			storageClassName = "nginx-sc-telemtery"

			scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			storageClassName = storagePolicyName
			scParameters[scParamStoragePolicyID] = profileID
			clusterDistributionValue = svClusterDistribution

			sc, err = client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		} else {
			ginkgo.By("Set Resource quota for GC")
			storageClassName = defaultNginxStorageClassName
			scParameters[svStorageClassName] = storagePolicyName
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
			clusterDistributionValue = tkgClusterDistribution

			scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		ginkgo.By("Creating StorageClass for Statefulset")

		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &sc.Name
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		if !windowsEnv {
			gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		}
		ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Get the list of Volumes attached to Pods before scale down.
		for _, sspod := range ssPodsBeforeScaleDown.Items {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					// Verify the attached volume match the one in CNS cache.
					volumeID := pv.Spec.CSI.VolumeHandle

					if guestCluster {
						volumeID = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
					}

					// Verify the attached volume has cluster-distribution value set.
					ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult for PVC with VolumeID: %s",
						pv.Spec.CSI.VolumeHandle))
					queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volumeID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(len(queryResult.Volumes) > 0).To(gomega.BeTrue())

					for _, queryRes := range queryResult.Volumes[0].Metadata.ContainerClusterArray {
						if queryRes.ClusterFlavor == clusterFlavor {
							framework.Logf("Cluster-distribution value on CNS is %s",
								queryRes.ClusterDistribution)
							gomega.Expect(queryRes.ClusterDistribution).Should(
								gomega.Equal(clusterDistributionValue), "Wrong/empty cluster-distribution name present on CNS")
						}
					}

				}
			}
		}
		replicas = 0
		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
		_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
		ssPodsAfterScaleDown, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	})
})
