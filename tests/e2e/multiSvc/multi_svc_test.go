/*
	Copyright 2024 The Kubernetes Authors.

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

package multiSvc

import (
	"context"
	"fmt"
	"os"
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/env"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var e2eTestConfig *config.E2eTestConfig

var _ = ginkgo.Describe("[csi-multi-svc] Multi-SVC", func() {
	var (
		namespaces            []string
		numberOfSvc           int
		clients               []clientset.Interface
		errors                []error
		storagePolicyNames    [2]string
		scParametersList      [2]map[string]string
		csiNamespace          string
		dataCenter            string
		scaleUpReplicaCount   int32
		scaleDownReplicaCount int32
		kubeconfig            string
		kubeconfig1           string
		computeCluster        string
		computeClusterPaths   []string
		supervisorIds         []string
		wcpServiceAccUsers    []string
		datastoreName         string
		datastoreIP           string
		sshClientConfig       *ssh.ClientConfig
		isHostRemoved         bool
		isHostInMM            bool
		isDsUnmountedFromHost bool
		isDsMountedOnSvc1     bool
		isDsMountedOnSvc2     bool
		hostToBeRemoved       string
		hostPath              string
		hostsInCluster        []*object.HostSystem
	)
	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		//getting list of clientset and namespace for both svc
		clients, namespaces, errors = k8testutil.GetMultiSvcClientAndNamespace()
		if len(errors) > 0 {
			framework.Failf("Unable get client and namespace for supervisor clusters")
		}

		e2eTestConfig = bootstrap.Bootstrap()

		//getting total number of supervisor clusters and list of their compute cluster path
		var err error
		numberOfSvc, computeClusterPaths, err = GetSvcCountAndComputeClusterPath(e2eTestConfig)
		framework.ExpectNoError(err, "Unable to find any compute cluster")
		if !(numberOfSvc > 0) {
			framework.Failf("Unable to find any supervisor cluster")
		}
		// list of storage policy for both the supervisors
		envStoragePolicyNameForSharedDatastoresList := []string{constants.EnvStoragePolicyNameForSharedDsSvc1,
			constants.EnvStoragePolicyNameForSharedDsSvc2}
		for i := 0; i < numberOfSvc; i++ {
			storagePolicyNames[i] = env.GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastoresList[i])
			profileID := vcutil.GetSpbmPolicyID(storagePolicyNames[i], e2eTestConfig)
			scParameters := make(map[string]string)
			scParametersList[i] = scParameters
			// adding profileID to storageClass param - StoragePolicyID
			scParametersList[i][constants.ScParamStoragePolicyID] = profileID
			scParametersList[i][constants.ScParamFsType] = constants.Ext4FSType
		}

		// Checking for any ready and schedulable node
		for i := 0; i < numberOfSvc; i++ {
			nodeList, err := fnodes.GetReadySchedulableNodes(ctx, clients[i])
			framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
			if !(len(nodeList.Items) > 0) {
				framework.Failf("Unable to find ready and schedulable Node")
			}
		}

		// Getting all env variables here
		csiNamespace = env.GetAndExpectStringEnvVar(constants.EnvCSINamespace)
		dataCenter = env.GetAndExpectStringEnvVar(constants.Datacenter)
		computeCluster = env.GetAndExpectStringEnvVar(constants.EnvComputeClusterName)
		datastoreName = env.GetAndExpectStringEnvVar(constants.EnvNfsDatastoreName)
		datastoreIP = env.GetAndExpectStringEnvVar(constants.EnvNfsDatastoreIP)
		kubeconfig = env.GetAndExpectStringEnvVar("KUBECONFIG")
		kubeconfig1 = env.GetAndExpectStringEnvVar("KUBECONFIG1")

		ginkgo.By("Getting User and Supervisor-Id for both the supervisors")
		// Iterating through number of svc to read it's config secret to get supervisor id and service account user
		wcpServiceAccUsers = []string{}
		for i := 0; i < numberOfSvc; i++ {
			vsphereCfg, err := k8testutil.GetSvcConfigSecretData(clients[i], ctx, csiNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			supervisorIds = append(supervisorIds, vsphereCfg.TestInput.Global.SupervisorID)
			// Getting service account user without domain name after spilittin it by @
			wcpServiceAccUsers = append(wcpServiceAccUsers, strings.Split(string(vsphereCfg.TestInput.Global.User), "@")[0])
		}

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(constants.NimbusVcPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var timeout int32 = 300
		// move back host to cluster
		if isHostRemoved {
			ginkgo.By("Moving host back to the cluster 1")
			err := MoveHostToCluster(e2eTestConfig, computeClusterPaths[0], hostToBeRemoved)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// exit host from MM
		if isHostInMM {
			ginkgo.By("Exit host from MM")
			k8testutil.ExitHostMM(ctx, hostsInCluster[0], timeout)
		}
		// unmount ds from cluster1
		if isDsMountedOnSvc1 {
			ginkgo.By("Remove mounted datastore from supervisor cluster 1")
			err := UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig, datastoreName, computeClusterPaths[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// unmount ds from cluster2
		if isDsMountedOnSvc2 {
			ginkgo.By("Remove mounted datastore from supervisor cluster 2")
			err := UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig, datastoreName, computeClusterPaths[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		// mount datastore back to host
		if isDsUnmountedFromHost {
			ginkgo.By("Mount back datastore to host in the supervisor cluster 1")
			err := MountNfsDatastoreOnClusterOrHost(e2eTestConfig, datastoreName, datastoreIP, hostPath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// sts,pvc cleanup
		for i := 0; i < numberOfSvc; i++ {
			// Changing kubeconfig for second supervisor
			var err error
			if i == 1 {
				os.Setenv(constants.KubeconfigEnvVar, kubeconfig1)
				framework.TestContext.KubeConfig = kubeconfig1
				clients[i], err = k8testutil.CreateKubernetesClientFromConfig(kubeconfig1)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Error creating k8s client with %v: %v", kubeconfig1, err))
			}

			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespaces[i]))
			fss.DeleteAllStatefulSets(ctx, clients[i], namespaces[i])
			ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespaces[i]))
			err = clients[i].CoreV1().Services(namespaces[i]).Delete(ctx, constants.ServiceName, *metav1.NewDeleteOptions(0))
			if !apierrors.IsNotFound(err) {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By(fmt.Sprintf("Deleting all PVCs in namespace: %v", namespaces[i]))
			pvcList := k8testutil.GetAllPVCFromNamespace(clients[i], namespaces[i])
			for _, pvc := range pvcList.Items {
				framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, clients[i], pvc.Name, namespaces[i]),
					"Failed to delete PVC", pvc.Name)
			}

			// perfrom cleanup of old stale entries of pv if left in the setup
			pvs, err := clients[i].CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if len(pvs.Items) != 0 {
				for _, pv := range pvs.Items {
					gomega.Expect(clients[i].CoreV1().PersistentVolumes().Delete(ctx, pv.Name,
						*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
				}
			}

			k8testutil.SetResourceQuota(clients[i], namespaces[i], constants.DefaultrqLimit)

			/* resetting and performing cleanup of kubeconfig export variable so that for
			next testcase it should pickup the default svc kubeconfig set in the env. export variable */
			if i == numberOfSvc-1 {
				os.Setenv(constants.KubeconfigEnvVar, kubeconfig)
				framework.TestContext.KubeConfig = kubeconfig
				// setting it to first/default kubeconfig
				clients[0], err = k8testutil.CreateKubernetesClientFromConfig(kubeconfig)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

	})

	/* TESTCASE-1

	Create multiple SVCs and create workloads on it

	Steps:
		1. Verify the SVC account is different for each cluster
		2. Create sts on each of the clusters and verify that it is successful
		3. Delete all the sts and pvcs which belong to them
	*/

	ginkgo.It("[csi-multi-svc] Workload creation on each of the clusters",
		ginkgo.Label(constants.P0, constants.Wcp, constants.MultiSvc, constants.Vc80), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var err error
			// Iterating through number of svc to create sts
			for n := 0; n < numberOfSvc; n++ {
				client := clients[n]
				namespace := namespaces[n]
				// Changing kubeconfig for second supervisor
				if n == 1 {
					os.Setenv(constants.KubeconfigEnvVar, kubeconfig1)
					framework.TestContext.KubeConfig = kubeconfig1
					client, err = k8testutil.CreateKubernetesClientFromConfig(kubeconfig1)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(),
						fmt.Sprintf("Error creating k8s client with %v: %v", kubeconfig1, err))
				}

				ginkgo.By("Create StatefulSet with 3 replicas with parallel pod management")
				service, statefulset, err := k8testutil.CreateStatefulSetAndVerifyPVAndPodNodeAffinty(ctx, client,
					e2eTestConfig, namespace, true, 3, false, nil, false, false, true, "", nil, false,
					storagePolicyNames[n])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer func() {
					fss.DeleteAllStatefulSets(ctx, client, namespace)
					k8testutil.DeleteService(namespace, client, service)
				}()

				framework.Logf("Scale up sts replica count to 5")
				scaleUpReplicaCount = 5
				err = k8testutil.ScaleUpStatefulSetPod(ctx, client, e2eTestConfig, statefulset, namespace,
					scaleUpReplicaCount, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				framework.Logf("Scale down sts replica count to 1")
				scaleDownReplicaCount = 1
				err = k8testutil.ScaleDownStatefulSetPod(ctx, e2eTestConfig, client, statefulset, namespace,
					scaleDownReplicaCount, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

	ginkgo.It("[csi-multi-svc] Verify volume lifecycle ops post password rotation",
		ginkgo.Label(constants.P0, constants.Wcp, constants.MultiSvc, constants.Vc80), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var pvclaim *v1.PersistentVolumeClaim
			var err error
			// Iteration through each of the svc
			for i := 0; i < numberOfSvc; i++ {
				client := clients[i]
				namespace := namespaces[i]
				if i == 1 {
					os.Setenv(constants.KubeconfigEnvVar, kubeconfig1)
					framework.TestContext.KubeConfig = kubeconfig1
					client, err = k8testutil.CreateKubernetesClientFromConfig(kubeconfig1)
					gomega.Expect(err).NotTo(gomega.HaveOccurred(),
						fmt.Sprintf("Error creating k8s client with %v: %v", kubeconfig1, err))
				}

				ginkgo.By("Create StatefulSet with 3 replicas with parallel pod management")
				service, statefulset, err := k8testutil.CreateStatefulSetAndVerifyPVAndPodNodeAffinty(ctx, client,
					e2eTestConfig, namespace, true, 3, false, nil, false, false, true, "", nil, false,
					storagePolicyNames[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer func() {
					fss.DeleteAllStatefulSets(ctx, client, namespace)
					k8testutil.DeleteService(namespace, client, service)
				}()

				ginkgo.By("Perform password rotation on the supervisor")
				passwordRotated, err := k8testutil.PerformPasswordRotationOnSupervisor(client, ctx, csiNamespace,
					e2eTestConfig.TestInput.TestBedInfo.VcAddress, e2eTestConfig)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(passwordRotated).To(gomega.BeTrue())

				// scaling up/down sts created before password rotation
				framework.Logf("Scale up sts replica count to 5")
				scaleUpReplicaCount = 5
				err = k8testutil.ScaleUpStatefulSetPod(ctx, client, e2eTestConfig, statefulset, namespace,
					scaleUpReplicaCount, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				framework.Logf("Scale down sts replica count to 1")
				scaleDownReplicaCount = 1
				err = k8testutil.ScaleDownStatefulSetPod(ctx, e2eTestConfig, client, statefulset, namespace,
					scaleDownReplicaCount, true)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Create a Pvc and attach a pod to it
				_, pvclaim, err = k8testutil.CreatePVCAndStorageClass(ctx, e2eTestConfig, client, namespace, nil,
					scParametersList[i], "", nil, "", false, "", storagePolicyNames[i])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer func() {
					err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}()

				// Waiting for PVC to be bound
				var pvclaims []*v1.PersistentVolumeClaim
				var volHandle string
				pvclaims = append(pvclaims, pvclaim)
				ginkgo.By("Waiting for pvc to be in bound state")
				persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims,
					framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pv := persistentvolumes[0]
				volHandle = pv.Spec.CSI.VolumeHandle
				// Create a Pod to use this PVC, and verify volume has been attached
				ginkgo.By("Creating pod to attach PV to the node")
				pod, err := k8testutil.CreatePod(ctx, e2eTestConfig, client, namespace, nil,
					[]*v1.PersistentVolumeClaim{pvclaim}, false, constants.ExecCommand)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer func() {
					ginkgo.By("Deleting the pod")
					err = fpod.DeletePodWithWait(ctx, client, pod)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}()

				var vmUUID string
				var exists bool
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
				annotations := pod.Annotations
				vmUUID, exists = annotations[constants.VmUUIDLabel]
				gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", constants.VmUUIDLabel))
				framework.Logf("VMUUID : %s", vmUUID)
				isDiskAttached, err := vcutil.IsVolumeAttachedToVM(client, e2eTestConfig, volHandle, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			}

		})

	/* TESTCASE-3

	verify permissions of the service account

	Steps:
		1. Create a SVC cluster with enough storage quota for the test with shared, local and vsan datastores
		2. Verify permissions for the service account on root folder, compute cluster, hosts and datastores
		3. Delete the SVC cluster
	*/

	ginkgo.It("[csi-multi-svc] Verify permissions of the service account",
		ginkgo.Label(constants.P0, constants.Wcp, constants.MultiSvc, constants.Vc80), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ginkgo.By("Verify permission on root folder for each of the wcp service account users")
			for _, user := range wcpServiceAccUsers {
				framework.Logf("Verifying permission on root folder for user : %s", user)
				userPermission, err := VerifyPermissionForWcpStorageUser(ctx, e2eTestConfig, "RootFolder", "", user,
					constants.RoleCnsSearchAndSpbm)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(userPermission).To(gomega.BeTrue(), "user permission is not valid for root folder")
			}

			ginkgo.By("Verify permission on clusters for each of the wcp service account users")
			// creating array of roles for both service account users as per the desired cluster permission
			roles := [][]string{
				{constants.RoleCnsHostConfigStorageAndCnsVm, ""},
				{"", constants.RoleCnsHostConfigStorageAndCnsVm},
			}
			// iterating through compute cluster paths
			for i, path := range computeClusterPaths {
				role := roles[i%2] // Alternates between the two roles
				// iterating through service account users
				for j, user := range wcpServiceAccUsers {
					framework.Logf("Verifying permission on root folder for user: %s", wcpServiceAccUsers[i])
					userPermission, err := VerifyPermissionForWcpStorageUser(ctx, e2eTestConfig, "Cluster", path, user, role[j])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(userPermission).To(gomega.BeTrue(), "user permission is not valid for compute-cluster path")
				}
			}

			ginkgo.By("Verify service account permission on each of the datastore")
			// Getting list of all datastores
			dataCenters, err := vcutil.GetAllDatacenters(ctx, e2eTestConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			datastores, err := GetDatastoreNamesFromDCs(sshClientConfig, e2eTestConfig, dataCenters)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Iterating thorugh datastores to verify permission for svc account users
			for _, datastorePath := range datastores {
				// roleForUser array to store roles for both svc account user based on datastore
				var roleForUser []string
				switch {
				case strings.Contains(datastorePath, "vsanDatastore (2)"):
					roleForUser = []string{constants.RoleCnsDatastore, ""}
				case strings.Contains(datastorePath, "nfs") || strings.Contains(datastorePath, "sharedVmfs"):
					roleForUser = []string{constants.RoleCnsDatastore, constants.RoleCnsDatastore}
				case strings.Contains(datastorePath, "vsanDatastore (1)"):
					roleForUser = []string{"", constants.RoleCnsDatastore}
				default: // for "local-0"
					roleForUser = []string{"", ""}
				}

				// iterating through service account users
				for j, user := range wcpServiceAccUsers {
					userPermission, err := VerifyPermissionForWcpStorageUser(ctx, e2eTestConfig, "Datastore",
						datastorePath, user, roleForUser[j])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(userPermission).To(gomega.BeTrue(), "user permission is not valid for datastore")
				}
			}
		})

	/* TESTCASE-4

	Verify that alarm an alarm is raised when a shared datastore becomes non-shared

	Steps:
		1. add a nfs shared datastore
		2. verify that required permissions are set for the datastore
		3. unmount nfs datastore from one of the hosts say host1 and verify that
		   an alarm is raised indicating that a shared datastore is no longer shared now
		4. remove the host1 from the cluster
		5. verify that the alarm from step 4 is cleared
		6. add host1 back to cluster and don't mount nfs datastore on it yet and verify
		   that an alarm is raised again indicating that a shared datastore is no longer
		   shared across all hosts in the cluster now
		7. mount nfs datastore on host1 and verify that the alarm from step 7 is gone
	*/

	ginkgo.It("[csi-multi-svc] Verify that an alarm is raised when a shared datastore "+
		"becomes non-shared", ginkgo.Label(constants.P0, constants.Wcp, constants.MultiSvc, constants.Vc80), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var timeout int32 = 300
		var alarmPresent bool

		ginkgo.By("Adding a shared datastore to supervisor cluster 1")
		err := MountNfsDatastoreOnClusterOrHost(e2eTestConfig, datastoreName, datastoreIP, computeClusterPaths[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDsMountedOnSvc1 = true
		defer func() {
			if isDsMountedOnSvc1 {
				ginkgo.By("Remove mounted datastore from supervisor cluster 1")
				err = UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig, datastoreName, computeClusterPaths[0])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isDsMountedOnSvc1 = false
			}
		}()

		datastorePath := dataCenter + "/datastore/" + datastoreName
		ginkgo.By("Verify datastore has permission for storage service account from supervisor cluster 1")
		userPermission, err := VerifyPermissionForWcpStorageUser(ctx, e2eTestConfig, "Cluster", datastorePath,
			wcpServiceAccUsers[0], constants.RoleCnsDatastore)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(userPermission).To(gomega.BeTrue(), "user permission is not changed for datastore "+
			"in supervisor cluster 1")

		ginkgo.By("Unmount datastore from one of the host from supervisor cluster 1")
		clusterComputeResource, _, err := vcutil.GetClusterName(ctx, e2eTestConfig)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		hostsInCluster = vcutil.GetHostsByClusterName(ctx, clusterComputeResource, computeCluster)
		hostIP1, err := hostsInCluster[0].ManagementIPs(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		hostToBeRemoved = hostIP1[0].String()
		hostPath = computeClusterPaths[0] + "/" + hostToBeRemoved
		framework.Logf("Unmount datastore from host : %v", hostToBeRemoved)
		err = UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig, datastoreName, hostToBeRemoved)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDsUnmountedFromHost = true
		defer func() {
			if isDsUnmountedFromHost {
				ginkgo.By("Remove mounted datastore from host in the supervisor cluster 1")
				err = MountNfsDatastoreOnClusterOrHost(e2eTestConfig, datastoreName, datastoreIP, hostPath)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isDsUnmountedFromHost = false
			}
		}()

		ginkgo.By("Verify an alarm is raised for unmounted datastore and host in the supervisor cluster 1")
		alarm := "Datastore no longer accessible to all hosts in the cluster compute resource"
		alarmPresent, err = IsAlarmPresentOnDatacenter(ctx, e2eTestConfig, dataCenter, alarm, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(alarmPresent).To(gomega.BeTrue())

		ginkgo.By("Remove host from the cluster from STEP 3 and Verify alarm has disappeared")
		vcutil.EnterHostIntoMM(ctx, hostsInCluster[0], constants.EnsureAccessibilityMModeType, timeout, false)
		isHostInMM = true
		defer func() {
			if isHostInMM {
				vcutil.ExitHostMM(ctx, hostsInCluster[0], timeout)
				isHostInMM = false
			}
		}()

		isHostRemoved, err := RemoveEsxiHostFromCluster(e2eTestConfig, dataCenter, computeCluster, hostToBeRemoved)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isHostRemoved).To(gomega.BeTrue(), "Host was not removed from cluster")
		defer func() {
			if isHostRemoved {
				ginkgo.By("Adding host back to the cluster 1")
				err = MoveHostToCluster(e2eTestConfig, computeClusterPaths[0], hostToBeRemoved)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				isHostRemoved = false
			}
		}()

		alarmPresent, err = IsAlarmPresentOnDatacenter(ctx, e2eTestConfig, dataCenter, alarm, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(alarmPresent).To(gomega.BeTrue())

		ginkgo.By("Add host back to cluster and Verify alarm has appeared again")
		err = MoveHostToCluster(e2eTestConfig, computeClusterPaths[0], hostToBeRemoved)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isHostRemoved = false
		vcutil.ExitHostMM(ctx, hostsInCluster[0], timeout)
		isHostInMM = false
		alarmPresent, err = IsAlarmPresentOnDatacenter(ctx, e2eTestConfig, dataCenter, alarm, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(alarmPresent).To(gomega.BeTrue())

		ginkgo.By("Mount datastore back to host in the cluster and Verify alarm has disappeared")
		err = MountNfsDatastoreOnClusterOrHost(e2eTestConfig, datastoreName, datastoreIP, hostPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDsUnmountedFromHost = false
		alarmPresent, err = IsAlarmPresentOnDatacenter(ctx, e2eTestConfig, dataCenter, alarm, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(alarmPresent).To(gomega.BeTrue())
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

	ginkgo.It("[csi-multi-svc] Move a shared datastore from one SVC to another and check permission",
		ginkgo.Label(constants.P0, constants.Wcp, constants.MultiSvc, constants.Vc80), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var roleForSvcUser []string

			ginkgo.By("Adding a shared datastore to supervisor cluster 1")
			err := MountNfsDatastoreOnClusterOrHost(e2eTestConfig, datastoreName, datastoreIP, computeClusterPaths[0])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isDsMountedOnSvc1 = true
			defer func() {
				if isDsMountedOnSvc1 {
					ginkgo.By("Remove mounted datastore from supervisor cluster 1")
					err = UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig, datastoreName, computeClusterPaths[0])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDsMountedOnSvc1 = false
				}
			}()

			datastorePath := "/" + dataCenter + "/datastore/" + datastoreName
			ginkgo.By("Verify datastore has permission for storage service account from supervisor cluster 1")
			roleForSvcUser = []string{constants.RoleCnsDatastore, ""}
			// iterating through service account users
			for j, user := range wcpServiceAccUsers {
				userPermission, err := VerifyPermissionForWcpStorageUser(ctx, e2eTestConfig, "Datastore",
					datastorePath, user, roleForSvcUser[j])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(userPermission).To(gomega.BeTrue())
			}

			ginkgo.By("Adding same shared datastore to supervisor cluster 2")
			err = MountNfsDatastoreOnClusterOrHost(e2eTestConfig, datastoreName, datastoreIP, computeClusterPaths[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isDsMountedOnSvc2 = true
			defer func() {
				if isDsMountedOnSvc2 {
					ginkgo.By("Remove mounted datastore from supervisor cluster 2")
					err = UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig, datastoreName, computeClusterPaths[1])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDsMountedOnSvc2 = false
				}
			}()

			ginkgo.By("Verify datastore has permission for storage service account from both the supervisor clusters")
			roleForSvcUser = []string{constants.RoleCnsDatastore, constants.RoleCnsDatastore}
			// iterating through service account users
			for j, user := range wcpServiceAccUsers {
				userPermission, err := VerifyPermissionForWcpStorageUser(ctx, e2eTestConfig, "Datastore",
					datastorePath, user, roleForSvcUser[j])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(userPermission).To(gomega.BeTrue())
			}

			ginkgo.By("Removing mounted shared datastore from supervisor cluster 1")
			err = UnMountNfsDatastoreFromClusterOrHost(e2eTestConfig, datastoreName, computeClusterPaths[1])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			isDsMountedOnSvc2 = false
			ginkgo.By("Verify datastore has permission for storage service account from the svc1 but not from svc2")
			roleForSvcUser = []string{constants.RoleCnsDatastore, ""}
			// iterating through service account users
			for j, user := range wcpServiceAccUsers {
				userPermission, err := VerifyPermissionForWcpStorageUser(ctx, e2eTestConfig, "Datastore",
					datastorePath, user, roleForSvcUser[j])
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(userPermission).To(gomega.BeTrue())
			}
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

	ginkgo.It("[csi-multi-svc] Kill VC session from a service account and attempt CSI ops from "+
		"the corresponding SVC", ginkgo.Label(constants.P0, constants.Wcp, constants.MultiSvc, constants.Vc80), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var oldSessionIds [][]string
		var err error
		ginkgo.By("Getting VC session Id for both the supervisors")
		for i := 0; i < numberOfSvc; i++ {
			// getting session ids for each svc
			sessionIDs, err := GetVcSessionIDsforSupervisor(e2eTestConfig, supervisorIds[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Storing it in oldSessionIds to validate later
			oldSessionIds = append(oldSessionIds, sessionIDs)
		}

		ginkgo.By("Kill VC session from svc1")
		err = KillVcSessionIDs(e2eTestConfig, oldSessionIds[0])
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify VC session Ids are changed for svc1 but not for svc2")
		for i := 0; i < numberOfSvc; i++ {
			isSessionIdSame, err := WaitAndCompareSessionIDList(ctx, e2eTestConfig, supervisorIds[i], oldSessionIds[i])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// for first supervisor session id will change after killing vc session
			if i == 0 {
				gomega.Expect(isSessionIdSame).To(gomega.BeFalse())
			} else {
				gomega.Expect(isSessionIdSame).To(gomega.BeTrue())
			}
		}

		ginkgo.By("Verify sts creation is successful on each of the clusters")
		for n := 0; n < numberOfSvc; n++ {
			// Storing it here as these values will not change in defer method
			client := clients[n]
			namespace := namespaces[n]
			// Changing kubeconfig for second supervisor
			if n == 1 {
				os.Setenv(constants.KubeconfigEnvVar, kubeconfig1)
				framework.TestContext.KubeConfig = kubeconfig1
				client, err = k8testutil.CreateKubernetesClientFromConfig(kubeconfig1)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					fmt.Sprintf("Error creating k8s client with %v: %v", kubeconfig1, err))
			}

			ginkgo.By("Create StatefulSet with 3 replica with parallel pod management")
			service, statefulset, err := k8testutil.CreateStatefulSetAndVerifyPVAndPodNodeAffinty(ctx, client,
				e2eTestConfig, namespace, true, 3, false, nil, false, false, true, "", nil, false,
				storagePolicyNames[n])
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				fss.DeleteAllStatefulSets(ctx, client, namespace)
				k8testutil.DeleteService(namespace, client, service)
			}()

			framework.Logf("Scale up sts replica count to 5")
			scaleUpReplicaCount = 5
			err = k8testutil.ScaleUpStatefulSetPod(ctx, client, e2eTestConfig, statefulset, namespace, scaleUpReplicaCount,
				true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Scale down sts replica count to 1")
			scaleDownReplicaCount = 1
			err = k8testutil.ScaleDownStatefulSetPod(ctx, e2eTestConfig, client, statefulset, namespace, scaleDownReplicaCount,
				true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})
})
