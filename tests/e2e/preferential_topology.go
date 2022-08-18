/*
Copyright 2022 The Kubernetes Authors.

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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"golang.org/x/crypto/ssh"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[preferential-topology] Preferential-Topology-Level2", func() {
	f := framework.NewDefaultFramework("preferential-topology-aware-provisioning")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                   clientset.Interface
		namespace                string
		zoneValues               []string
		regionValues             []string
		allZones                 []string
		allRegions               []string
		pvZone                   string
		pvRegion                 string
		allowedTopologies        []v1.TopologySelectorLabelRequirement
		err                      error
		nodeList                 *v1.NodeList
		bindingMode              storagev1.VolumeBindingMode
		sshClientConfig          *ssh.ClientConfig
		vcAddress                string
		allMasterIps             []string
		masterIp                 string
		vCenterPort              string
		vCenterUIUser            string
		vCenterUIPassword        string
		ClusterdatastoreListMap  map[string]string
		dataCenters              []string
		clusters                 []string
		datastoreNames           []string
		csiReplicas              int32
		csiNamespace             string
		pod                      *v1.Pod
		pvclaim                  *v1.PersistentVolumeClaim
		storageclass             *storagev1.StorageClass
		preferredDatastoreChosen int
		shareddatastoreListMap   map[string]string
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		// fetching k8s master ip
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		// fetching vcenter details
		vcAddress = e2eVSphere.Config.Global.VCenterHostname
		vCenterPort = e2eVSphere.Config.Global.VCenterPort
		vCenterUIUser = e2eVSphere.Config.Global.User
		vCenterUIPassword = e2eVSphere.Config.Global.Password

		// create category required for selecting datastore preference
		createPreferredDatastoreCategory(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp)

		// fetching datacenter details
		dataCenters, err = getDataCenterDetails(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching cluster details
		clusters, err = getClusterDetails(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, dataCenters)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching csi pods replicas
		csiNamespace = GetAndExpectStringEnvVar(envCSINamespace)
		csiDeployment, err := client.AppsV1().Deployments(csiNamespace).Get(
			ctx, vSphereCSIControllerPodNamePrefix, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		csiReplicas = *csiDeployment.Spec.Replicas

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(client, namespace)
		ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
		err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		Testcase-1:
			Tag single datastore and verify it is honored

			Steps
			1. Assign Tag of 'zone-1', Category "cns.vmware.topology-preferred-datastores" to datastore specific to
			region-1/zone-1.
			2. Create Storage Class SC-1 with WFC binding mode and allowed topologies set to region-1 and zone-1.
			3. Create PVC using storage class SC-1.
			4. Wait for PVC to reach Bound state.
			5. Describe PV and verify node affinity details should contain both region-1 and zone-1 details.
			6. Verify volume should be provisioned on the selected preferred datastore of region-1 and zone-1.
			7. Create Pod from PVC created above.
			8. Wait for Pod to reach running state.
			9. Make sure Pod is running on the same node as mentioned in the node affinity details.
			10. Assign Tag of 'zone-2', Category "cns.vmware.topology-preferred-datastores" to datastore specific to
			region-2/zone-2.
			10. Create Storage Class SC-2 with Immedaite binding mode and allowed topologies set to region-2 and zone-2.
			11. Create StatefulSet pods using default pod management policy with replica 3 using SC-2
			12. Wait for StatefulSet pods to come up.
			13. Wait for sts PVC to raech Bound state and verify node affinity details should contain
			both region-2 and zone-2 details.
			15. Verify volume should be provisioned on the selected preferred datastore of region-2 and zone-2.
			16. Make sure sts pods are running on the same node as mentioned in the node affinity details
			17. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			18. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag single datastore as a preference and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-1/zone-1
		topologyDomainForRegion1Zone1 := GetAndExpectStringEnvVar(envTopologyDomainForRegion1Zone1)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyDomainForRegion1Zone1)

		// fetching list of datastores available in region-1/zone-1
		ginkgo.By("Fetching list of datastores available in region-1/zone-1")
		ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, clusters[0])

		// Create tag for preferred datastore
		ginkgo.By("Create tag for datastore chosen as preference for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-1/zone-1")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen,
			ClusterdatastoreListMap)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		// Creating Storage Class and PVC
		ginkgo.By("Creating Storage class and standalone PVC")
		storageclass, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, nil, "",
			allowedTopologies, bindingMode, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//Wait for PVC to reach Bound state
		ginkgo.By("Expect claim to provision volume successfully")
		pvs1, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")
		pv1 := pvs1[0]
		defer func() {
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, pvclaim.Namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv1.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// verify volume provisioning in allowed topologies
		ginkgo.By("Verify if volume is provisioned in specified zone and region")
		if allowedTopologies == nil {
			zoneValues = allZones
			regionValues = allRegions
		}
		pvRegion, pvZone, err = verifyVolumeTopology(pv1, zoneValues, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Creating Pod and verifying volume is attached to the node
		ginkgo.By("Creating a pod")
		pod, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume:%s is attached to the node: %s",
			pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID1 := getNodeUUID(ctx, client, pod.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv1.Spec.CSI.VolumeHandle, vmUUID1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By("Deleting the pod and wait for disk to detach")
			err := fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv1.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		}()

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStandalonePods(ctx, client, pod, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// verifying Pod scheduleding on the the node
		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
		err = verifyPodLocation(pod, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// fetching topology domains of region-2/zone-2
		topologyDomainForRegion2Zone2 := GetAndExpectStringEnvVar(envTopologyDomainForRegion2Zone2)
		regionValues, zoneValues, allowedTopologies := topologyParameterForStorageClass(topologyDomainForRegion2Zone2)

		// fetching list of datastores available in region-2/zone-2
		ginkgo.By("Fetching list of datastores available in region-2/zone-2")
		ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, clusters[1])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-1/zone-1")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, zoneValues[1], preferredDatastoreChosen,
			ClusterdatastoreListMap)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies, "", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume provisioning
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)
	})

	/*
		Testcase-2:
			Tag multiple datastore specific to region-1/zone-1 and verify it is honored

			Steps
			1. Assign Tag of "zone-1', Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to region-1/zone-1 (ex- NFS-1)
			2. Assign Tag of "zone-1', Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to region-1/zone-1 (ex- sharedVMFS-12)
			3. Create SC with WFC binding mode and allowed topologies set to region-1/zone-1 in the SC
			4. Create StatefulSet with replica 3 with the above SC
			5. Wait for all the StatefulSet to come up
			6. Wait for PV , PVC to bound and POD to reach running state
			7. Describe PV and verify node affinity details should contain both region-1/zone-1  details.
			8. Verify volume should be provisioned on any of the preferred datastores of region-1/zone-1.
			9. Make sure POD is running on the same node as mentioned in the node affinity details
			10. Perform Scaleup of StatefulSet. Increase the replica count from 3 to 7.
			11. Wait for all the StatefulSet to come up
			12. Wait for PV , PVC to bound and POD to reach running state
			13. Describe PV and verify node affinity details should contain both region-1/zone-1 details.
			14. Verify volume should be provisioned on any of the preferred datastores of region-1/zone-1.
			15. Make sure POD is running on the same node as mentioned in the node affinity details
			16. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			17. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag multiple datastores as a preference in zone-1 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-1/zone-1
		topologyDomainForRegion1Zone1 := GetAndExpectStringEnvVar(envTopologyDomainForRegion1Zone1)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyDomainForRegion1Zone1)

		// fetching list of datastores available in region-1/zone-1
		ginkgo.By("Fetching list of datastores available in region-1/zone-1")
		ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, clusters[0])

		// Create tag for preferred datastore
		ginkgo.By("Create tag for datastore chosen as preference for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-1/zone-1")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, ClusterdatastoreListMap)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			v1.PersistentVolumeReclaimPolicy(bindingMode), "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)

		// perform statefulset scaleup
		replicas = 7
		ginkgo.By("Scale up statefulset replica count from 3 to 7")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)
	})

	/*
		Testcase-3:
			Tag multiple datastore specific to region-3/zone-3 and verify it is honored

			Steps
			1. Assign Tag of zone-3, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to region-3/zone-3 (ex- NFS-2)
			2. Assign Tag zone-3, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to region-3/zone-3 (ex- vSAN-2)
			3. Assign Tag zone-3, Category "cns.vmware.topology-preferred-datastores" to datastore
			shared across region-1/zone-1,region-2/zone-2,region-3/zone-3 (ex- NFS-12)
			4. Create SC with Immediate binding mode and allowed topologies set to region-3/zone-3 in the SC
			5. Create StatefulSet with parallel pod management policy and replica 3 with the above SC
			6. Wait for all the StatefulSet to come up
			7. Wait for PV , PVC to bound and POD to reach running state
			8. Describe PV and verify node affinity details should contain both region-1 and site-2 details.
			9. Verify volume should be provisioned on any of the preferred datastores of region-3/zone-3
			10. Perform Scaleup of StatefulSet. Increase the replica count from 3 to 10
			11. Wait for all the StatefulSet to come up
			12. Wait for PV , PVC to bound and POD to reach running state
			13. Describe PV and verify node affinity details should contain both region-3/zone-3 details.
			14. Verify volume should be provisioned on any of the preferred datastores of region-3/zone-3
			15. Make sure POD is running on the same node as mentioned in the node affinity details
			16. Perform ScaleDown of StatefulSet. Decrease the replica count from 10 to 5
			17. Verify scaledown operation went successful.
			18. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			19. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag multiple datastores as a preference shared across zones and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 3

		// fetching topology domains of region-3/zone-3
		topologyDomainForRegion3Zone3 := GetAndExpectStringEnvVar(envTopologyDomainForRegion3Zone3)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyDomainForRegion3Zone3)

		// fetching list of datastores available in region-3/zone-3
		ginkgo.By("Fetching list of datastores available in region-3/zone-3")
		ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, clusters[2])

		// Create tag for preferred datastore
		ginkgo.By("Create tag for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[2])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-1/zone-1")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[2], preferredDatastoreChosen, ClusterdatastoreListMap)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)

		// perform statefulset scaleup
		replicas = 10
		ginkgo.By("Scale up statefulset replica count from 3 to 10")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)

		replicas = 5
		scaleDownStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)
	})

	/*
		Testcase-4:
			Tag preferred datastore to region-2/zone-2 and the datastore is accessible
			across entire cluster and verify it is honored

			Steps
			1. Assign Tag zone-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across entire zones. (ex - sharedVMFS-12)
			2. Create SC with WFC binding mode and allowed topologies set to region-2/zone-2 in the SC
			3. Create StatefulSet with replica 3 with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV , PVC to bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain both region-2/zone-2 details.
			7. Verify volume provisioning should be on the preferred datastore.
			8. Make sure POD is running on the same node as mentioned in node affinity details
			9. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			10. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag single datastore as a preference shared across zones and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-2/zone-2
		topologyDomainForRegion2Zone2 := GetAndExpectStringEnvVar(envTopologyDomainForRegion2Zone2)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyDomainForRegion2Zone2)

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap = getListOfSharedDatastoresBetweenVMs(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp)

		// Create tag for preferred datastore
		ginkgo.By("Create tag for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-2/zone-2")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, shareddatastoreListMap)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			v1.PersistentVolumeReclaimPolicy(bindingMode), "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			shareddatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)
	})

	/*
		Testcase-5:
			Multiple tags are assigned to Shared datastore with single allowed topology

			Steps
			1. Assign Tag zone-1, Category "cns.vmware.topology-preferred-datastores" to
			datastore accessible across entire zones (ex - sharedVMFS-12)
			2. Assign Tag zone-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across entire zones (ex - sharedVMFS-12)
			3. Create SC with WFC binding mode and allowed topologies set to region-1 and
			site-1/site-2 in the SC
			4. Create StatefulSet with replica 3 with the above SC
			5. Wait for all the StatefulSet to come up
			6. Wait for PV , PVC to bound and POD to reach running state
			7. Describe PV and verify node affinity details.
			8. Verify volume provisioning should be on the preferred datastore.
			9. Make sure POD is running on the same node as mentioned in node affinity details
			10. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			11. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag shared datastore as a preference which is shared across zones with "+
		"allowed topology set to single zone and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-2/zone-2
		topologyDomainForRegion2Zone2 := GetAndExpectStringEnvVar(envTopologyDomainForRegion2Zone2)
		_, zoneValuesZone2, _ := topologyParameterForStorageClass(topologyDomainForRegion2Zone2)

		// fetching topology domains of region-3/zone-3
		topologyDomainForRegion3Zone3 := GetAndExpectStringEnvVar(envTopologyDomainForRegion3Zone3)
		regionValuesZone3, zoneValuesZone3, allowedTopologiesZone3 :=
			topologyParameterForStorageClass(topologyDomainForRegion3Zone3)

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap = getListOfSharedDatastoresBetweenVMs(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp)

		// Create tag of zone-2 for preferred datastore
		ginkgo.By("Create tag of zone-2 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone2[0])

		// Create tag of zone-3 for preferred datastore
		ginkgo.By("Create tag of zone-3 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone3[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-3/zone-3")
		_ = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone2[0], preferredDatastoreChosen, shareddatastoreListMap)

		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone3[0], preferredDatastoreChosen, shareddatastoreListMap)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValuesZone2[0])
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValuesZone3[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologiesZone3,
			v1.PersistentVolumeReclaimPolicy(bindingMode), "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			shareddatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValuesZone3,
			regionValuesZone3)
	})

	/*
		Testcase-6:
			Multiple tags are assigned to Shared datastore with multiple allowed topology

			Steps
			1. Assign Tag of zone-1, Category "cns.vmware.topology-preferred-datastores" to
			datastore accessible across entire zones (ex - sharedVMFS-12)
			2. Assign Tag of zone-2, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across entire zones (ex - sharedVMFS-12)
			3. Assign Tag of zone-3, Category "cns.vmware.topology-preferred-datastores" to datastore
			accessible across entire zones (ex - sharedVMFS-12)
			3. Create SC with WFC binding mode and allowed topologies set to region-1/zone-1 and
			region-2/zone-2 in the SC
			4. Create StatefulSet with replica 3 with the above SC
			5. Wait for all the StatefulSet to come up
			6. Wait for PV , PVC to bound and POD to reach running state
			7. Describe PV and verify node affinity details should contain the allowed topologies
			details specified in the SC
			8. Verify volume provisioning should be on the preferred datastore.
			9. Make sure POD is running on the same node as mentioned in node affinity details
			10. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			11. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag shared datastore as a preference which is shared across zones with "+
		"allowed topology set to multiple zones and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-1/zone-1
		topologyDomainForRegion1Zone1 := GetAndExpectStringEnvVar(envTopologyDomainForRegion1Zone1)
		_, zoneValuesZone1, _ := topologyParameterForStorageClass(topologyDomainForRegion1Zone1)

		// fetching topology domains of region-2/zone-2
		topologyDomainForRegion2Zone2 := GetAndExpectStringEnvVar(envTopologyDomainForRegion2Zone2)
		_, zoneValuesZone2, _ := topologyParameterForStorageClass(topologyDomainForRegion2Zone2)

		// fetching topology domains of region-3/zone-3
		topologyDomainForRegion3Zone3 := GetAndExpectStringEnvVar(envTopologyDomainForRegion3Zone3)
		regionValuesZone3, zoneValuesZone3, allowedTopologiesZone3 :=
			topologyParameterForStorageClass(topologyDomainForRegion3Zone3)

		// creating allowed topology for region-1/zone1, region-2/zone-2
		topologyValues := topologyDomainForRegion1Zone1 + "," + topologyDomainForRegion2Zone2
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyValues)

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap = getListOfSharedDatastoresBetweenVMs(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp)

		// Create tag of zone-1 for preferred datastore
		ginkgo.By("Create tag of zone-1 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone1[0])

		// Create tag of zone-2 for preferred datastore
		ginkgo.By("Create tag of zone-2 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone2[0])

		// Create tag of zone-3 for preferred datastore
		ginkgo.By("Create tag of zone-3 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone3[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-3/zone-3")
		_ = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone1[0], preferredDatastoreChosen, shareddatastoreListMap)

		_ = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone2[0], preferredDatastoreChosen, shareddatastoreListMap)

		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValuesZone3[0], preferredDatastoreChosen, shareddatastoreListMap)

		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValuesZone1[0])
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValuesZone2[0])
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValuesZone3[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologiesZone3,
			v1.PersistentVolumeReclaimPolicy(bindingMode), "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			shareddatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValuesZone3,
			regionValuesZone3)
	})

	/*
		Testcase-8:
			Change  datastore preference in zone-1 and verify it is honored

			Steps
			1. Assign Tag of zone-1, Category "cns.vmware.topology-preferred-datastores" to datastore
			specific to region-1/zone-1 (ex - vSAN-1)
			2. Create SC with Immediate Binding mode and allowed topologies set to region-1/zone-1
			in the SC
			3. Create StatefulSet sts1 with parallel pod management policy and replica 3
			with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV , PVC to bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain both
			region-1/zone-1 details.
			7. Verify volume should be provisioned on the preferred datastore of region-1/zone-1
			8. Make sure POD is running on the same node as mentioned in the node affinity details
			9. Remove tags from previously preferred datastore and assign tag of zone-1 and
			category "cns.vmware.topology-preferred-datastores" to new datastore shared
			across site-1 and site-2 (ex - sharedVMFS-12)
			10. Restart csi driver and wait for sometime for datastore preference to get changed.
			11. Create StatefulSet sts2 with replica 3 with the above SC
			12. Wait for all the StatefulSet to come up
			13. Wait for PV , PVC to bound and POD to reach running state
			14. Describe PV and verify node affinity details should contain both region-1/zone-1 details.
			15. Verify volume should be provisioned on the newly selected preferred datastore of region-1/zone-1
			16. Make sure POD is running on the same node as mentioned in the node affinity details
			17. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			18. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Change datatsore preference in zone-1 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-1/zone-1
		topologyDomainForRegion1Zone1 := GetAndExpectStringEnvVar(envTopologyDomainForRegion1Zone1)
		regionValues, zoneValues, allowedTopologies :=
			topologyParameterForStorageClass(topologyDomainForRegion1Zone1)

		// fetching list of datastores available in region-1/zone-1
		ginkgo.By("Fetching list of datastores available in region-1/zone-1")
		ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, clusters[0])

		// fetching list of datatstores shared between vm's
		shareddatastoreListMap = getListOfSharedDatastoresBetweenVMs(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp)

		// Create tag of zone-1  for preferred datastore
		ginkgo.By("Create tag of zone-1 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-1/zone-1")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, ClusterdatastoreListMap)

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)

		ginkgo.By("Remove the datastore preference chosen for volume provisioning")
		detachTagCreatedOnPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, datastoreNames[0], zoneValues[0])

		ginkgo.By("Choose a new datastore as a prefernce for volume provisioning")
		preferredDsList := changeDatastorePreference(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, shareddatastoreListMap,
			datastoreNames)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		// Creating statefulset1 with 3 replicas
		ginkgo.By("Creating statefulset1 with 3 replica")
		statefulset1 := GetStatefulSetFromManifest(namespace)
		statefulset1.Name = "sts-1"
		ginkgo.By("Creating statefulset1")
		CreateStatefulSet(namespace, statefulset1, client)
		replicas = *(statefulset1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset1, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset1, namespace, preferredDsList,
			shareddatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset1, namespace, zoneValues,
			regionValues)
	})

	/*
		Testcase-9:
		Change datastore in zone-2 and verify it is honored

			Steps
			1. Assign Tag of zone-2, Category "cns.vmware.topology-preferred-datastores" to
			datastore specific to region-2/zone-2 (ex - vSAN-2)
			2. Create SC with WFC and allowed topologies set to region-2/zone-2 in the SC
			3. Create StatefulSet sts1 with replica 3 with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV , PVC to bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain both region-2/zone-2 details.
			7. Verify volume should be provisioned on the selected preferred datastore of region-2/zone-2.
			8. Make sure POD is running on the same node as mentioned in node affinity details
			9. Remove tags from previously preferred datastore and assign tag of zone-2 and
			category "cns.vmware.topology-preferred-datastores" to new datastore specific to region-2/zone-2 (ex - NFS-2)
			10. Restart csi driver and wait for sometime for datastore preference to get changed.
			11. Create StatefulSet sts2 with replica 3 with the above SC
			12. Wait for all the StatefulSet to come up
			13. Describe PV and verify node affinity details should contain both region-2/zone-2 details.
			14. Verify volume should be provisioned on the newly selected preferred datastore of site-2
			15. Scaleup StatefulSet sts1 created in step3. Increase the replica count from 3 to 7.
			16. Wait for all the StatefulSet to come up
			17. Wait for PV , PVC to bound and POD to reach running state
			18. Describe PV and verify node affinity details should contain both region-2/zone-2 details.
			19. Verify volume should be provisioned on the newly selected preferred datastore of region-2/zone-2.
			20. Make sure POD is running on the same node as mentioned in the node affinity details
			21. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			22. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Change datatsore preference in zone-2 and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-2/zone-2
		topologyDomainForRegion2Zone2 := GetAndExpectStringEnvVar(envTopologyDomainForRegion2Zone2)
		regionValues, zoneValues, allowedTopologies :=
			topologyParameterForStorageClass(topologyDomainForRegion2Zone2)

		// fetching list of datastores available in region-2/zone-2
		ginkgo.By("Fetching list of datastores available in region-2/zone-2")
		ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, clusters[1])

		// Create tag of zone-2  for preferred datastore
		ginkgo.By("Create tag of zone-2 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-2/zone-2")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, ClusterdatastoreListMap)

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)

		ginkgo.By("Remove the datastore preference chosen for volume provisioning")
		detachTagCreatedOnPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, datastoreNames[0], zoneValues[0])

		ginkgo.By("Choose a new datastore as a prefernce for volume provisioning")
		preferredDsList := changeDatastorePreference(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, ClusterdatastoreListMap,
			datastoreNames)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		// Creating statefulset1 with 3 replicas
		ginkgo.By("Creating statefulset1 with 3 replica")
		statefulset1 := GetStatefulSetFromManifest(namespace)
		statefulset1.Name = "sts-1"
		ginkgo.By("Creating statefulset1")
		CreateStatefulSet(namespace, statefulset1, client)
		replicas = *(statefulset1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset1, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset1, namespace, preferredDsList,
			shareddatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset1, namespace, zoneValues,
			regionValues)

		// perform statefulset scaleup
		replicas = 7
		ginkgo.By("Scale up statefulset replica count from 3 to 7")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)
		datastoreNames = append(datastoreNames, preferredDsList...)

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset1, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset1, namespace, zoneValues,
			regionValues)
	})

	/*
		Testcase-10:
		Change  datastore preference  multiple times and  perform Scaleup/ScaleDown operation on StatefulSet

			Steps
			1. Assign Tag "site-1', Category "cns.vmware.topology-preferred-datastores" to datastore shared across site-1 and site-2 (ex- NFS-12)
			2. Create SC with WFC Binding mode and allowed topologies set to region-1 and site-1 in the SC
			3. Create StatefulSet with parallel pod management policy and replica 3 with the above SC
			4. Wait for all the StatefulSet to come up
			5. Wait for PV , PVC to bound and POD to reach running state
			6. Describe PV and verify node affinity details should contain both region-1 and site-1 details.
			7. Verify volume should be provisioned on the selected preferred datastore of site-1
			8. Make sure POD is running on the same node as mentioned in the node affinity details
			9. Remove tag from preferred datastore and assign tag 'site-1'  and category "cns.vmware.topology-preferred-datastores" to new datastore specific to site-1  (ex - vSAN-1).
			10. Wait for preference of datastore to be updated by default. (By default wait time is 5 mins)
			11. Perform Scaleup operation, Increase the replica count of StatefulSet from 3 to 13.
			12. Wait for all the StatefulSet to come up
			13. Wait for PV , PVC to bound and POD to reach running state
			14. Describe PV and verify node affinity details should contain both region-1 and site-1 details.
			15. Verify volume should be provisioned on the newly selected preferred datastore.
			16. Perform Scaledown operation. Decrease the replica count from 13 to 6.
			17. Remove tag from preferred datastore and assign tag 'site-1' and  category "cns.vmware.topology-preferred-datastores" to new datastore which is shared specific to site-1. (ex- NFS-1)
			18. Wait for preference of datastore to be updated by default. (By default wait time is 5 mins)
			19. Perform Scaleup operation again and increase the replica count from 6 to 20.
			20. Wait for all the StatefulSet to come up
			21. Wait for PV , PVC to bound and POD to reach running state
			22. Describe PV and verify node affinity details should contain both region-1 and site-1 details.
			23. Verify volume should be provisioned on the newly selected preferred datastore of site-1
			24. Make sure POD is running on the same node as mentioned in the node affinity details
			25. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
			26. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Change datastore preference multiple times and perform Scaleup/ScaleDown operation on StatefulSet", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		preferredDatastoreChosen = 1

		// fetching topology domains of region-2/zone-2
		topologyDomainForRegion2Zone2 := GetAndExpectStringEnvVar(envTopologyDomainForRegion2Zone2)
		regionValues, zoneValues, allowedTopologies :=
			topologyParameterForStorageClass(topologyDomainForRegion2Zone2)

		// fetching list of datastores available in region-2/zone-2
		ginkgo.By("Fetching list of datastores available in region-2/zone-2")
		ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser,
			vCenterUIPassword, sshClientConfig, masterIp, clusters[1])

		// Create tag of zone-2  for preferred datastore
		ginkgo.By("Create tag of zone-2 for datastore chosen for volume provisioning")
		createTagForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0])

		// choose preferred datastore
		ginkgo.By("Choosing datastore as a preference for volume provisioning in region-2/zone-2")
		datastoreNames = choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, ClusterdatastoreListMap)

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		ginkgo.By("Creating StorageClass for Statefulset")
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, nil, allowedTopologies,
			"", "", false)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Creating statefulset with 3 replicas
		ginkgo.By("Creating statefulset with 3 replica")
		statefulset := GetStatefulSetFromManifest(namespace)
		statefulset.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ginkgo.By("Creating statefulset")
		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset, namespace, zoneValues,
			regionValues)

		ginkgo.By("Remove the datastore preference chosen for volume provisioning")
		detachTagCreatedOnPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, datastoreNames[0], zoneValues[0])

		ginkgo.By("Choose a new datastore as a prefernce for volume provisioning")
		preferredDsList := changeDatastorePreference(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp, zoneValues[0], preferredDatastoreChosen, ClusterdatastoreListMap,
			datastoreNames)
		defer func() {
			deleteTagCreatedForPreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
				sshClientConfig, masterIp, zoneValues[0])
		}()

		/* Restarting csi driver and wait for sometime for csi driver to pick up the selected
		datastore as a preference choice */
		ginkgo.By("Restarting csi driver and wait for sometime for csi driver to pick up the " +
			"selected datastore as a preference choice")
		restartCSIDriver(ctx, client, namespace, csiReplicas)
		time.Sleep(preferredDsRefreshTimeInterval)

		// Creating statefulset1 with 3 replicas
		ginkgo.By("Creating statefulset1 with 3 replica")
		statefulset1 := GetStatefulSetFromManifest(namespace)
		statefulset1.Name = "sts-1"
		ginkgo.By("Creating statefulset1")
		CreateStatefulSet(namespace, statefulset1, client)
		replicas = *(statefulset1.Spec.Replicas)
		defer func() {
			framework.Logf("Deleting all statefulset in namespace: %v", namespace)
			fss.DeleteAllStatefulSets(client, namespace)
		}()

		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(client, statefulset1, replicas)
		gomega.Expect(fss.CheckMount(client, statefulset1, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown = fss.GetPodList(client, statefulset1)
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset1.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset1, namespace, preferredDsList,
			shareddatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset1, namespace, zoneValues,
			regionValues)

		// perform statefulset scaleup
		replicas = 7
		ginkgo.By("Scale up statefulset replica count from 3 to 7")
		scaleUpStatefulSetPod(ctx, client, statefulset, namespace, replicas, false)
		datastoreNames = append(datastoreNames, preferredDsList...)

		// verifying volume is provisioned on the preferred datastore
		ginkgo.By("Verify volume is provisioned on the specified datatsore")
		verifyVolumeProvisioningForStatefulSet(ctx, client, statefulset1, namespace, datastoreNames,
			ClusterdatastoreListMap)

		// Verify node and pv topology affinity should contains specified zone and region details of SC
		ginkgo.By("Verify node and pv topology affinity should contains specified zone and region details of SC")
		verifyPVnodeAffinityAndPODnodedetailsForStatefulsets(ctx, client, statefulset1, namespace, zoneValues,
			regionValues)
	})
})
