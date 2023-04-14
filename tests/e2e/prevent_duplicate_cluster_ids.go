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
	"strings"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fssh "k8s.io/kubernetes/test/e2e/framework/ssh"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Prevent duplicate cluster ID", func() {
	f := framework.NewDefaultFramework("cluster-id-test")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                        clientset.Interface
		namespace                     string
		csiNamespace                  string
		csiReplicas                   int32
		vCenterUIUser                 string
		vCenterUIPassword             string
		clusterId                     string
		revertToOriginalVsphereSecret bool
		vCenterIP                     string
		vCenterPort                   string
		dataCenter                    string
		scParameters                  map[string]string
		accessMode                    v1.PersistentVolumeAccessMode
		sshClientConfig               *ssh.ClientConfig
		nimbusGeneratedK8sVmPwd       string
	)

	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		scParameters = make(map[string]string)
		accessMode = v1.ReadWriteOnce
		// fetching required parameters

		csiNamespace = csiSystemNamespace
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

		vsphereCfg := getCSIConfigSecretData(client, ctx, csiNamespace)
		vCenterUIUser = vsphereCfg.Global.User
		vCenterUIPassword = vsphereCfg.Global.Password
		dataCenter = vsphereCfg.Global.Datacenters
		clusterId = vsphereCfg.Global.ClusterID
		vCenterIP = e2eVSphere.Config.Global.VCenterHostname
		vCenterPort = e2eVSphere.Config.Global.VCenterPort
		framework.Logf("clusterId: %v", clusterId)
		revertToOriginalVsphereSecret = false
		nimbusGeneratedK8sVmPwd = GetAndExpectStringEnvVar(nimbusK8sVmPwd)

		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(nimbusGeneratedK8sVmPwd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if !revertToOriginalVsphereSecret {
			ginkgo.By("Delete vsphere-csi-cluster-id configmap if it exists")
			_, err := client.CoreV1().ConfigMaps(csiNamespace).Get(ctx,
				vsphereClusterIdConfigMapName, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				err = client.CoreV1().ConfigMaps(csiNamespace).Delete(ctx,
					vsphereClusterIdConfigMapName, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			ginkgo.By("Reverting back to original vsphere secret")
			framework.Logf("clusterId: %v", clusterId)
			recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				clusterId, vCenterPort, dataCenter, csiReplicas)
		}
	})

	/*
		Generate unique cluster id through configmap and create workloads
		1. Create vsphere config secret with no cluster id field.
		2. Validate that "vsphere-csi-cluster-id" configmap is generated with a unique cluster id.
		3. Create statefulset with replica 3 and a deployment.
		4. Verify all PVCs are in bound state and pods are in running state.
		5. Scale sts replica to 5.
		6. Verify cns metadata and check if cluster id is populated in cns metadata.
		7. Clean up the sts, deployment, pods and PVCs.

	*/
	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Generate unique cluster id through configmap"+
		" and create workloads", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")

		ginkgo.By("Creating csi config secret with no cluster id field set")
		recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
			"", vCenterPort, dataCenter, csiReplicas)

		defer func() {
			if !revertToOriginalVsphereSecret {
				ginkgo.By("Delete vsphere-csi-cluster-id configmap if it exists")
				_, err := client.CoreV1().ConfigMaps(csiNamespace).Get(ctx,
					vsphereClusterIdConfigMapName, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					err = client.CoreV1().ConfigMaps(csiNamespace).Delete(ctx,
						vsphereClusterIdConfigMapName, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				ginkgo.By("Reverting back to original vsphere secret")
				framework.Logf("clusterId: %v", clusterId)
				recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
					clusterId, vCenterPort, dataCenter, csiReplicas)
			}
			revertToOriginalVsphereSecret = true
		}()

		ginkgo.By("Verify cluster id configmap is auto generated by csi driver")
		verifyClusterIdConfigMapGeneration(client, ctx, csiNamespace, true)
		clusterID := fetchClusterIdFromConfigmap(client, ctx, csiNamespace)
		framework.Logf("clusterID: %v", clusterID)

		ginkgo.By("Creating Storage Class")
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
		}
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		// Check if it is file volumes setups
		if rwxAccessMode {
			accessMode = v1.ReadWriteMany
		}
		ginkgo.By("Creating statefulset with replica 3 and a deployment")
		statefulset, deployment, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "", accessMode)
		replicas := *(statefulset.Spec.Replicas)

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

		// Scale up replicas of statefulset and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas+2, true, true)
		verifyVolumeMetadataOnDeployments(ctx, client, deployment, namespace, nil, nil,
			nil, "")
		err = checkClusterIdValueOnWorkloads(&e2eVSphere, client, ctx, namespace, clusterID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

	})

	/*
		Modify unique cluster id value in Configmap
		1. Create vsphere config secret with no cluster id field.
		2. Validate that "vsphere-csi-cluster-id" configmap is generated with a unique cluster id.
		3. Change the cluster id value in "vsphere-csi-cluster-id" configmap, which should throw a proper error.

	*/
	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Modify unique cluster id value in"+
		" Configmap", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")

		ginkgo.By("Creating csi config secret with no cluster id field set")
		recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
			"", vCenterPort, dataCenter, csiReplicas)

		defer func() {
			if !revertToOriginalVsphereSecret {
				ginkgo.By("Delete vsphere-csi-cluster-id configmap if it exists")
				_, err := client.CoreV1().ConfigMaps(csiNamespace).Get(ctx,
					vsphereClusterIdConfigMapName, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					err = client.CoreV1().ConfigMaps(csiNamespace).Delete(ctx,
						vsphereClusterIdConfigMapName, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				ginkgo.By("Reverting back to original vsphere secret")
				framework.Logf("clusterId: %v", clusterId)
				recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
					clusterId, vCenterPort, dataCenter, csiReplicas)
			}
			revertToOriginalVsphereSecret = true
		}()

		ginkgo.By("Verify cluster id configmap is auto generated by csi driver")
		verifyClusterIdConfigMapGeneration(client, ctx, csiNamespace, true)

		ginkgo.By("Modify unique cluster id value in Configmap")
		clusterIdCm, err := client.CoreV1().ConfigMaps(csiNamespace).Get(ctx, vsphereClusterIdConfigMapName,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		clusterIdCm.Data["clusterID"] = "cluster1"
		_, err = client.CoreV1().ConfigMaps(csiNamespace).Update(ctx, clusterIdCm,
			metav1.UpdateOptions{})
		framework.Logf("Error from updating cluster id value in configmap is : %v", err.Error())
		gomega.Expect(err).To(gomega.HaveOccurred())

	})

	/*
		Generate cluster id and then set cluster id in vsphere config secret
		and remove cluster id field in vsphere config secret
		1. Create vsphere config secret with no cluster id field.
		2. Validate that "vsphere-csi-cluster-id" configmap is generated with a unique cluster id.
		3. Create statefulset with replica 3 and a deployment.
		4. Verify all PVCs are in bound state and pods are in running state.
		5. Delete config secret and create config secret with cluster id set and restart csi driver.
		6. Check if "vsphere-csi-cluster-id" configmap still exists.
		7. Verify CSI pods go into crashing state with a proper error message.
		8. Remove cluster id field from vsphere config secret and restart csi driver.
		9. Verify csi pods are in running state.
		10. Scale sts replica to 5.
		11. Verify cns metadata and check if cluster id is populated in cns metadata.
		12. Clean up the sts, deployment, pods and PVCs.

	*/
	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Generate cluster id and then set cluster id "+
		"in vsphere config secret and remove cluster id field in vsphere config secret", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")

		ginkgo.By("Creating csi config secret with no cluster id field set")
		recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
			"", vCenterPort, dataCenter, csiReplicas)
		defer func() {
			if !revertToOriginalVsphereSecret {
				ginkgo.By("Delete vsphere-csi-cluster-id configmap if it exists")
				_, err := client.CoreV1().ConfigMaps(csiNamespace).Get(ctx,
					vsphereClusterIdConfigMapName, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					err = client.CoreV1().ConfigMaps(csiNamespace).Delete(ctx,
						vsphereClusterIdConfigMapName, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				ginkgo.By("Reverting back to original vsphere secret")
				framework.Logf("clusterId: %v", clusterId)
				recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
					clusterId, vCenterPort, dataCenter, csiReplicas)
			}
			revertToOriginalVsphereSecret = true
		}()

		ginkgo.By("Verify cluster id configmap is auto generated by csi driver")
		verifyClusterIdConfigMapGeneration(client, ctx, csiNamespace, true)
		clusterID := fetchClusterIdFromConfigmap(client, ctx, csiNamespace)
		framework.Logf("clusterID: %v", clusterID)

		ginkgo.By("Creating Storage Class")
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
		}
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		// Check if it is file volumes setups
		if rwxAccessMode {
			accessMode = v1.ReadWriteMany
		}
		ginkgo.By("Creating statefulset with replica 3 and a deployment")
		statefulset, deployment, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "", accessMode)
		replicas := *(statefulset.Spec.Replicas)

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

		ginkgo.By("Creating csi config secret with cluster id field set")
		createCsiVsphereSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace,
			vCenterIP, vCenterPort, dataCenter, "", "cluster1")

		ginkgo.By("Restart CSI driver")
		_, err = restartCSIDriver(ctx, client, csiNamespace, csiReplicas)
		gomega.Expect(err).To(gomega.HaveOccurred())
		ginkgo.By("Verify that cluster id configmap still exists")
		verifyClusterIdConfigMapGeneration(client, ctx, csiNamespace, true)

		ginkgo.By("Check if csi pods are in crashing state after recreation of secret with proper message")
		csipods, err := client.CoreV1().Pods(csiNamespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		k8sMasterIPs := getK8sMasterIPs(ctx, client)
		k8sMasterIP := k8sMasterIPs[0]
		var csiPodName string
		for _, csiPod := range csipods.Items {
			if strings.Contains(csiPod.Name, vSphereCSIControllerPodNamePrefix) {
				csiPodName = csiPod.Name
				break
			}
		}
		errMessage := "Please remove the cluster ID from vSphere Config Secret."
		grepCmdForErrMsg := "echo `kubectl logs " + csiPodName + " -n " +
			csiSystemNamespace + " --allContainers" + " | grep " + "'" + errMessage

		framework.Logf("Invoking command '%v' on host %v", grepCmdForErrMsg,
			k8sMasterIP)
		result, err := sshExec(sshClientConfig, k8sMasterIP,
			grepCmdForErrMsg)
		if err != nil || result.Code != 0 {
			fssh.LogResult(result)
			gomega.Expect(err).To(gomega.HaveOccurred(), fmt.Sprintf("couldn't execute command: %s on host: %v , error: %s",
				grepCmdForErrMsg, k8sMasterIP, err))
		}
		if result.Stdout != "" {
			framework.Logf("CSI pods are in crashing state with proper error message")
		} else {
			framework.Logf("CSI pods are in crashing state with improper error message")
			gomega.Expect(err).To(gomega.HaveOccurred())
		}

		ginkgo.By("Remove cluster id field from vsphere config secret and verify csi pods are in running state")
		recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
			"", vCenterPort, dataCenter, csiReplicas)
		newclusterID := fetchClusterIdFromConfigmap(client, ctx, csiNamespace)
		if clusterID != newclusterID {
			framework.Failf("New clusterID should not be generated")
		}
		csipods, err = client.CoreV1().Pods(csiNamespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodsRunningReady(client, csiNamespace, int32(csipods.Size()), 0, pollTimeout, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up replicas of statefulset and verify CNS entries for volumes")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas+2, true, true)
		verifyVolumeMetadataOnDeployments(ctx, client, deployment, namespace, nil, nil,
			nil, "")
		err = checkClusterIdValueOnWorkloads(&e2eVSphere, client, ctx, namespace, clusterID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

	})

	/*
		Recreate vsphere config secret multiple times
		1. Create vsphere config secret with no cluster id field.
		2. Validate that "vsphere-csi-cluster-id" configmap is generated with a unique cluster id.
		3. Create statefulset with replica 3 and a deployment.
		4. Verify all PVCs are in bound state and pods are in running state.
		5. Delete and create vsphere config secret multiple times(3-4 times atleast continuously).
		6. Verify "vsphere-csi-cluster-id" configmap still exists with same cluster id.
		7. Scale sts replica to 5.
		8. Verify cns metadata and check if cluster id is populated in cns metadata.
		9. Clean up the sts, deployment, pods and PVCs.

	*/
	ginkgo.It("[csi-config-secret-block][csi-config-secret-file] Recreate vsphere config secret multiple"+
		" times", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		framework.Logf("CNS_TEST: Running for vanilla k8s setup")

		ginkgo.By("Creating csi config secret with no cluster id field set")
		recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
			"", vCenterPort, dataCenter, csiReplicas)

		defer func() {
			if !revertToOriginalVsphereSecret {
				ginkgo.By("Delete vsphere-csi-cluster-id configmap if it exists")
				_, err := client.CoreV1().ConfigMaps(csiNamespace).Get(ctx,
					vsphereClusterIdConfigMapName, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					err = client.CoreV1().ConfigMaps(csiNamespace).Delete(ctx,
						vsphereClusterIdConfigMapName, metav1.DeleteOptions{})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				ginkgo.By("Reverting back to original vsphere secret")
				framework.Logf("clusterId: %v", clusterId)
				recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
					clusterId, vCenterPort, dataCenter, csiReplicas)
			}
			revertToOriginalVsphereSecret = true
		}()

		ginkgo.By("Verify cluster id configmap is auto generated by csi driver")
		verifyClusterIdConfigMapGeneration(client, ctx, csiNamespace, true)
		clusterID := fetchClusterIdFromConfigmap(client, ctx, csiNamespace)
		framework.Logf("clusterID: %v", clusterID)

		ginkgo.By("Creating Storage Class")
		if rwxAccessMode {
			scParameters[scParamFsType] = nfs4FSType
		}
		sc, err := createStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Delete Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating service")
		service := CreateService(namespace, client)
		defer func() {
			deleteService(namespace, client, service)
		}()
		// Check if it is file volumes setups
		if rwxAccessMode {
			accessMode = v1.ReadWriteMany
		}
		ginkgo.By("Creating statefulset with replica 3 and a deployment")
		statefulset, deployment, _ := createStsDeployment(ctx, client, namespace, sc, true,
			false, 0, "", accessMode)
		replicas := *(statefulset.Spec.Replicas)

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

		ginkgo.By("Recreating CSI config secret multiple times to verify if a" +
			"new cluster id configmap gets auto generated")
		for i := 0; i < 3; i++ {
			recreateVsphereConfigSecret(client, ctx, vCenterUIUser, vCenterUIPassword, csiNamespace, vCenterIP,
				"", vCenterPort, dataCenter, csiReplicas)
		}
		newclusterID := fetchClusterIdFromConfigmap(client, ctx, csiNamespace)
		if clusterID != newclusterID {
			framework.Failf("New clusterID should not be generated")
		}

		ginkgo.By("Scale up replicas of statefulset and verify CNS entries for volumes")
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			replicas+2, true, true)
		verifyVolumeMetadataOnDeployments(ctx, client, deployment, namespace, nil, nil,
			nil, "")
		err = checkClusterIdValueOnWorkloads(&e2eVSphere, client, ctx, namespace, clusterID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		scaleDownNDeleteStsDeploymentsInNamespace(ctx, client, namespace)

	})

})
