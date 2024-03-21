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
	"reflect"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-multi-svc] Multi-SVC", func() {
	f := framework.NewDefaultFramework("multi-svc-statefulset")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespaces          []string
		numberOfSvc         int
		clients             []clientset.Interface
		storagePolicyNames  [2]string
		scParametersList    [2]map[string]string
		scParameters        map[string]string
		storageClassNames   [2]string
		csiNamespace        string
		dataCenter          string
		kubeconfig1         string
		computeClusterPaths []string
		vcAddress           string
		supervisorIds       []string
		wcpServiceAccUser   []string
	)
	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		numberOfSvc = 2
		clients, namespaces = getMultiSvcClientAndNamespace()
		envStoragePolicyNameForSharedDatastoresList := []string{envStoragePolicyNameForSharedDatastores, envStoragePolicyNameForSharedDatastores1}
		for i := 0; i <= numberOfSvc-1; i++ {
			storagePolicyNames[i] = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastoresList[i])
			setResourceQuota(clients[i], namespaces[i], rqLimit)
			scParameters = make(map[string]string)
			scParametersList[i] = scParameters
		}

		bootstrap()

		for i := 0; i <= numberOfSvc-1; i++ {
			nodeList, err := fnodes.GetReadySchedulableNodes(clients[i])
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
		}

		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		computeClusterPaths = getSvcComputeClusterPath()
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		dataCenter = GetAndExpectStringEnvVar(datacenter)
		kubeconfig1 = GetAndExpectStringEnvVar("KUBECONFIG1")
		framework.Logf("datacenter name is : %v", dataCenter)

		ginkgo.By("Getting User and Supervisor-Id for both the supervisors")
		for i := 0; i <= numberOfSvc-1; i++ {
			currentSecret, err := clients[i].CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalConf := string(currentSecret.Data[vsphereCloudProviderConfiguration])

			vsphereCfg, err := readConfigFromSecretString(originalConf)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			supervisorIds = append(supervisorIds, vsphereCfg.Global.SupervisorID)
			wcpServiceAccUser = append(wcpServiceAccUser, strings.Split(string(vsphereCfg.Global.User), "@")[0])
		}

	})

	ginkgo.AfterEach(func() {
		framework.Logf("log-after-each-1")
		/*
			clients, namespaces = getMultiSvcClientAndNamespace()
			for i := 0; i <= numberOfSvc-1; i++ {
				framework.Logf("log-after-each-2-1")
				fss.DeleteAllStatefulSets(clients[i], namespaces[i])
				framework.Logf("log-after-each-2-2")
				setResourceQuota(clients[i], namespaces[i], defaultrqLimit)
				framework.Logf("log-after-each-2-3")
				dumpSvcNsEventsOnTestFailure(clients[i], namespaces[i])
				framework.Logf("log-after-each-2-4")
			}
		*/
		framework.Logf("log-after-each-3")
	})

	/* TESTCASE-1

	Create multiple SVCs and create workloads on it

	Steps:
		1. Verify the SVC account is different for each cluster
		2. Create sts on each of the clusters and verify that it is successful
		3. Delete all the sts and pvcs which belong to them
	*/

	ginkgo.It("[csi-multi-svc] Workload creation on each of the clusters", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP multiSVC setup")
		}
		for n := 0; n < numberOfSvc; n++ {
			storageClassNames[n] = defaultNginxStorageClassName
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyNames[n])
			scParametersList[n][scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(clients[n], namespaces[n], rqLimit, storageClassNames[n])
			c1 := clients[n]
			ns1 := namespaces[n]
			scSpec := getVSphereStorageClassSpec(storageClassNames[n], scParametersList[n], nil, "", "", false)
			sc, err := clients[n].StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err := c1.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			ginkgo.By("Creating service")
			service := CreateService(namespaces[n], clients[n])
			defer func() {
				deleteService(ns1, c1, service)
			}()

			statefulset := GetStatefulSetFromManifest(namespaces[n])
			ginkgo.By("Updating replicas to 3 and podManagement Policy as Parallel")
			*(statefulset.Spec.Replicas) = 3
			statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &storageClassNames[n]
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &storageClassNames[n]
			CreateStatefulSet(namespaces[n], statefulset, clients[n])
			replicas := *(statefulset.Spec.Replicas)
			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(clients[n], statefulset, replicas)
			if n == 1 {
				os.Setenv(kubeconfigEnvVar, kubeconfig1)
				framework.TestContext.KubeConfig = kubeconfig1
				clients[n], err = createKubernetesClientFromConfig(kubeconfig1)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Error creating k8s client with %v: %v", kubeconfig1, err))
				// framework.AfterReadingAllFlags(&framework.TestContext)
			}
			// err = CheckMountForStsPods(clients[n], statefulset, mountPath)
			// gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ssPodsBeforeScaleDown := fss.GetPodList(clients[n], statefulset)
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			framework.Logf("Get the list of Volumes attached to Pods before scale down")
			// Get the list of Volumes attached to Pods before scale down
			var volumesBeforeScaleDown []string
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := clients[n].CoreV1().Pods(namespaces[n]).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(clients[n], statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}

			ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
			_, scaledownErr := fss.Scale(clients[n], statefulset, replicas-1)
			gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReadyReplicas(clients[n], statefulset, replicas-1)
			ssPodsAfterScaleDown := fss.GetPodList(clients[n], statefulset)
			gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			// After scale down, verify vSphere volumes are detached from deleted pods
			ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := clients[n].CoreV1().Pods(namespaces[n]).Get(ctx, sspod.Name, metav1.GetOptions{})
				if err != nil {
					gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
					for _, volumespec := range sspod.Spec.Volumes {
						if volumespec.PersistentVolumeClaim != nil {
							pv := getPvFromClaim(clients[n], statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
							if vanillaCluster {
								isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
									clients[n], pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName)
								gomega.Expect(err).NotTo(gomega.HaveOccurred())
								gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
									fmt.Sprintf("Volume %q is not detached from the node %q",
										pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
							} else {
								annotations := sspod.Annotations
								vmUUID, exists := annotations[vmUUIDLabel]
								gomega.Expect(exists).To(gomega.BeTrue(),
									fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

								ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
									pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
								ctx, cancel := context.WithCancel(context.Background())
								defer cancel()
								_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
								gomega.Expect(err).To(gomega.HaveOccurred(),
									fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
										vmUUID, sspod.Spec.NodeName))
							}
						}
					}
				}
			}

			// After scale down, verify the attached volumes match those in CNS Cache
			for _, sspod := range ssPodsAfterScaleDown.Items {
				_, err := clients[n].CoreV1().Pods(namespaces[n]).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(clients[n], statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}

			ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
			_, scaleupErr := fss.Scale(clients[n], statefulset, replicas)
			gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReplicas(clients[n], statefulset, replicas)
			fss.WaitForStatusReadyReplicas(clients[n], statefulset, replicas)

			ssPodsAfterScaleUp := fss.GetPodList(clients[n], statefulset)
			gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			// After scale up, verify all vSphere volumes are attached to node VMs.
			ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
			for _, sspod := range ssPodsAfterScaleUp.Items {
				err := fpod.WaitTimeoutForPodReadyInNamespace(clients[n], sspod.Name, statefulset.Namespace, pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pod, err := clients[n].CoreV1().Pods(namespaces[n]).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range pod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(clients[n], statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						ginkgo.By("Verify scale up operation should not introduced new volume")
						gomega.Expect(contains(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)).To(gomega.BeTrue())
						ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
							pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						var vmUUID string
						var exists bool
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						if vanillaCluster {
							vmUUID = getNodeUUID(ctx, clients[n], sspod.Spec.NodeName)
						} else {
							annotations := pod.Annotations
							vmUUID, exists = annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
							_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
							gomega.Expect(err).NotTo(gomega.HaveOccurred())
						}
						isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(clients[n], pv.Spec.CSI.VolumeHandle, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
						gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
						ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
						err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
			replicas = 0
			ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
			_, scaledownErr = fss.Scale(clients[n], statefulset, replicas)
			gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReplicas(clients[n], statefulset, replicas)
			ssPodsAfterScaleDown = fss.GetPodList(clients[n], statefulset)
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		}

	})

	/* TESTCASE-2

	verify volume lifecycle ops post password rotation

	Steps:
		1. Create sts on the cluster
		2. perform password rotation
		3. scale up and down the sts created in step 1
		4. Delete all the sts and pvcs which belong to them
		5. create a PVC and pod with it
		6. delete the pod and pvc created in step 6
	*/

	ginkgo.It("[csi-multi-svc] Verify volume lifecycle ops post password rotation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP multiSVC setup")
		}

		for i := 0; i <= numberOfSvc-1; i++ {
			currentSecret, err := clients[i].CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalConf := string(currentSecret.Data[vsphereCloudProviderConfiguration])
			vsphereCfg, err := readConfigFromSecretString(originalConf)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("User from Config Secret of svc: %v", vsphereCfg.Global.User)
			framework.Logf("Pwd from Config Secret of svc: %v", vsphereCfg.Global.Password)
			framework.Logf("Svc ID from Config Secret of svc: %v", vsphereCfg.Global.SupervisorID)

			//perform password rotation on supervisors
			err = performPasswordRotationOnSupervisor(vsphereCfg.Global.SupervisorID, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ret, err := verifyPasswordRotationOnSupervisor(clients[i], csiNamespace, ctx, vsphereCfg.Global.Password)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ret).To(gomega.BeTrue())
		}

	})

	/* TESTCASE-3

	verify permissions of the service account

	Steps:
		1. Create a SVC cluster with enough storage quota for the test with shared, local and vsan datastores
		2. Verify permissions for the service account on root folder, compute cluster, hosts and datastores
		3. Delete the SVC cluster
	*/

	ginkgo.It("[csi-multi-svc] Verify permissions of the service account", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP multiSVC setup")
		}

		for i := 0; i <= numberOfSvc-1; i++ {
			currentSecret, err := clients[i].CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalConf := string(currentSecret.Data[vsphereCloudProviderConfiguration])
			vsphereCfg, err := readConfigFromSecretString(originalConf)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("User from Config Secret of svc: %v", vsphereCfg.Global.User)

			verifyPermissionForWcpStorageUser("/WCPE2E_DC", vsphereCfg.Global.User, roleCnsSearchAndSpbm)
			verifyPermissionForWcpStorageUser("/WCPE2E_DC/host/wcpe2e-compute-cluster", vsphereCfg.Global.User, roleSupervisorServiceRootFolder)

			clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, "wcpe2e-compute-cluster")
			for _, host := range hostsInCluster {
				hostIP, err := host.ManagementIPs(ctx)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Host IP is: %v", hostIP[0].String())
				verifyPermissionForWcpStorageUser("/WCPE2E_DC/hosts/wcpe2e-compute-cluster", vsphereCfg.Global.User, roleCnsSearchAndSpbm)
			}
		}

	})

	/* TESTCASE-4

	Verify that alarm an alarm is raised when a shared datastore becomes non-shared

	Steps:
		1. add a nfs shared datastore
		2. verify that required permissions are set for the datastore
		3. unmount nfs datastore from one of the hosts say host1 and verify that an alarm is raised indicating that a shared datastore is no longer shared now
		4. remove the host1 from the cluster
		5. verify that the alarm from step 4 is cleared
		6. add host1 back to cluster and don't mount nfs datastore on it yet and verify that an alarm is raised again indicating that a shared datastore is no longer shared across all hosts in the cluster now
		7. mount nfs datastore on host1 and verify that the alarm from step 7 is gone
	*/

	ginkgo.It("[csi-multi-svc] Verify that alarm an alarm is raised when a shared datastore becomes non-shared", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP multiSVC setup")
		}
		var timeout int32 = 300
		ginkgo.By("Adding a shared datastore to supervisor cluster")
		computeCluster := GetAndExpectStringEnvVar(envComputeClusterName)
		datastoreName := GetAndExpectStringEnvVar(envNfsDatastoreName)
		datastoreIP := GetAndExpectStringEnvVar(envNfsDatastoreIP)

		mountDatastoreOnClusterOrHost(datastoreName, datastoreIP, computeClusterPaths[1])
		defer func() {
			ginkgo.By("Remove mounted datastore from cluster")
			UnMountDatastoreFromClusterOrHost(datastoreName, computeClusterPaths[1])
		}()
		time.Sleep(20 * time.Second)
		ginkgo.By("Verify datastore has permission for storage service account from supervisor cluster")
		userPermission, err := verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[0], roleCnsDatastore)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())
		ginkgo.By("Verify datastore has no permission for storage service account from supervisor cluster2")
		userPermission, err = verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[1], roleCnsSearchAndSpbm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())

		// Get Cluster details
		clusterComputeResource, _, err := getClusterName(ctx, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		hostsInCluster := getHostsByClusterName(ctx, clusterComputeResource, computeCluster)
		hostIP1, err := hostsInCluster[0].ManagementIPs(ctx)

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Host IP is: %v", hostIP1[0].String())
		ginkgo.By("Unmount datastore from one of the host from supervisor cluster")
		hostPath := computeClusterPaths[1] + "/" + hostIP1[0].String()
		UnMountDatastoreFromClusterOrHost(datastoreName, hostIP1[0].String())
		defer func() {
			mountDatastoreOnClusterOrHost(datastoreName, datastoreIP, hostPath)
		}()
		time.Sleep(1 * time.Minute)
		ginkgo.By("Verify an alarm is raised for unmounted datastore and host in the clsuter")
		err = verifyAlarmFromDatacenter()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Remove host from the cluster from STEP 3")
		enterHostIntoMM(ctx, hostsInCluster[0], ensureAccessibilityMModeType, timeout, false)

		removeEsxiHostFromCluster("wcpe2e-compute-cluster", hostIP1[0].String())
		ginkgo.By("Verify alarm has disappeared after removing host from cluster")
		err = verifyAlarmFromDatacenter()
		gomega.Expect(err).To(gomega.HaveOccurred())

		moveHostToCluster(computeClusterPaths[1], hostIP1[0].String())
		exitHostMM(ctx, hostsInCluster[0], timeout)
		ginkgo.By("Verify alarm has appeared after moving host to the same cluster")
		err = verifyAlarmFromDatacenter()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		mountDatastoreOnClusterOrHost(datastoreName, datastoreIP, hostPath)
		ginkgo.By("Verify alarm has disappeared after mounting datastore back to host in the cluster")
		err = verifyAlarmFromDatacenter()
		gomega.Expect(err).To(gomega.HaveOccurred())
		UnMountDatastoreFromClusterOrHost(datastoreName, computeClusterPaths[1])

	})

	/* TESTCASE-5

	Move a shared datastore from one SVC to another SVC and check permissions

	Steps:
		1. Verify the SVC account is different for each cluster
		2. Added a shared datastore say ds1 to svc1
		3. verify that ds1 has permissions for storage service account from svc1
		4. Added ds1 to svc2
		5. verify that ds1 has permissions for storage service accounts from both svc1 and svc2
		6. remove ds1 from svc1
		7. verify that ds1 has permissions for storage service account from svc1 alone

	*/

	ginkgo.It("[csi-multi-svc] Move a shared datastore from one SVC to another and check permission", func() {
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP multiSVC setup")
		}

		ginkgo.By("Adding a shared datastore to supervisor cluster")
		clusterPath := GetAndExpectStringEnvVar(envComputeClusterName)
		datastoreName := GetAndExpectStringEnvVar(envNfsDatastoreName)
		datastoreIP := GetAndExpectStringEnvVar(envNfsDatastoreIP)
		mountDatastoreOnClusterOrHost(datastoreName, datastoreIP, clusterPath)
		defer func() {
			ginkgo.By("Remove mounted datastore from cluster")
			UnMountDatastoreFromClusterOrHost(datastoreName, clusterPath)
		}()

		time.Sleep(20 * time.Second)
		ginkgo.By("Verify datastore has permission for storage service account from supervisor cluster")
		userPermission, err := verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[0], roleCnsDatastore)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())
		ginkgo.By("Verify datastore has no permission for storage service account from supervisor cluster2")
		userPermission, err = verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[1], roleCnsSearchAndSpbm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())

		ginkgo.By("Adding same shared datastore to supervisor cluster1")
		clusterPath1 := GetAndExpectStringEnvVar(envComputeClusterName1)
		mountDatastoreOnClusterOrHost(datastoreName, datastoreIP, clusterPath1)
		defer func() {
			ginkgo.By("Remove mounted datastore from cluster")
			UnMountDatastoreFromClusterOrHost(datastoreName, clusterPath1)
		}()
		ginkgo.By("Verify datastore has permission for storage service account from supervisor cluster")
		userPermission, err = verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[0], roleCnsDatastore)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())
		ginkgo.By("Verify datastore has permission for storage service account from supervisor cluster1 also")
		userPermission, err = verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[1], roleCnsDatastore)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())

		ginkgo.By("Removing mounted shared datastore from supervisor cluster1")
		UnMountDatastoreFromClusterOrHost(datastoreName, clusterPath1)
		ginkgo.By("Verify datastore has permission for storage service account from supervisor cluster")
		userPermission, err = verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[0], roleCnsDatastore)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())
		ginkgo.By("Verify datastore has no permission for storage service account from supervisor cluster1")
		userPermission, err = verifyPermissionForWcpStorageUser(datastoreName, wcpServiceAccUser[1], roleCnsSearchAndSpbm)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue())
	})

	/* TESTCASE-6

	Kill VC session from a service account and attempt CSI ops from the corresponding SVC

	Steps:
		1. Verify the SVC account is different for each cluster
		2. Verify different VC sessions are created for each SVC
		3. Kill VC sessions from svc1 and verify sessions from svc2 are not affected
		4. Create sts on each of the clusters and verify that they are successful
		5. Verify new VC sessions from svc1 are available.
		6. Delete all the sts and pvcs which belong to them

	*/

	ginkgo.It("[csi-multi-svc] Kill VC session from a service account and attempt CSI ops from the corresponding SVC", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP multiSVC setup")
		}

		var oldSessionIds [][]string
		var newSessionIds [][]string
		var supervisorIds []string
		ginkgo.By("Getting VC session Id for both the supervisors")
		for i := 0; i <= numberOfSvc-1; i++ {
			currentSecret, err := clients[i].CoreV1().Secrets(csiNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			originalConf := string(currentSecret.Data[vsphereCloudProviderConfiguration])
			vsphereCfg, err := readConfigFromSecretString(originalConf)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("SupervisorID from Config Secret of svc: %v", vsphereCfg.Global.SupervisorID)
			supervisorIds = append(supervisorIds, vsphereCfg.Global.SupervisorID)
			sessionIDs := getVcSessionIDforSupervisor(supervisorIds[i])
			oldSessionIds = append(oldSessionIds, sessionIDs)
		}

		ginkgo.By("Kill VC session from supervisor1")
		killVcSessionIDs(oldSessionIds[0])
		time.Sleep(90 * time.Second)

		ginkgo.By("Getting VC session Id for both supervisors after killing VC session from supervisor1")
		for i := 0; i <= numberOfSvc-1; i++ {
			framework.Logf("Get new vCenter session id")
			sessionIDs := getVcSessionIDforSupervisor(supervisorIds[i])
			newSessionIds = append(newSessionIds, sessionIDs)
			framework.Logf("Comparing old and new session ids from supervisor : %v", supervisorIds[i])
			if i == 0 {
				gomega.Expect(reflect.DeepEqual(oldSessionIds[i], newSessionIds[i])).To(gomega.BeFalse())
			} else {
				gomega.Expect(reflect.DeepEqual(oldSessionIds[i], newSessionIds[i])).To(gomega.BeTrue())
			}
		}

	})
})
