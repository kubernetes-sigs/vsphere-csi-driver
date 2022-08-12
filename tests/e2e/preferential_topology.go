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
		client            clientset.Interface
		namespace         string
		zoneValues        []string
		regionValues      []string
		allZones          []string
		allRegions        []string
		pvZone            string
		pvRegion          string
		allowedTopologies []v1.TopologySelectorLabelRequirement
		// pod               *v1.Pod
		// pvclaim           *v1.PersistentVolumeClaim
		// storageclass      *storagev1.StorageClass
		err               error
		nodeList          *v1.NodeList
		bindingMode       storagev1.VolumeBindingMode
		sshClientConfig   *ssh.ClientConfig
		vcAddress         string
		allMasterIps      []string
		masterIp          string
		vCenterPort       string
		vCenterUIUser     string
		vCenterUIPassword string
		//ClusterdatastoreListMap map[string]string
		//dataCenters []string
		//clusters                []string
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
		allMasterIps = getK8sMasterIPs(ctx, client)
		masterIp = allMasterIps[0]
		sshClientConfig = &ssh.ClientConfig{
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.Password(k8sVmPasswd),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		vcAddress = e2eVSphere.Config.Global.VCenterHostname
		vCenterPort = e2eVSphere.Config.Global.VCenterPort
		vCenterUIUser = e2eVSphere.Config.Global.User
		vCenterUIPassword = e2eVSphere.Config.Global.Password

		createPreferredDatastoreCategory(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
			sshClientConfig, masterIp)

		// dataCenters, err = getDataCenterDetails(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
		// 	sshClientConfig, masterIp)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// clusters, err = getClusterDetails(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
		// 	sshClientConfig, masterIp, dataCenters)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// getListOfSharedDatastoresBetweenVMs(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
		// 	sshClientConfig, masterIp)

		// datastoreListMap = getListOfAvailableDatastores(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
		// 	sshClientConfig, masterIp)
		// fmt.Println(datastoreListMap)

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
		Tag single datastore and verify it is honored

		Steps
		1. Assign Tag of 'site-1', Category "cns.vmware.topology-preferred-datastores" to datastore specific to site-1. (ex- NFS-1)
		2. Create Storage Class SC1 with WFC binding mode and allowed topologies set to region-1 and site-1.
		3. Create PVC-1
		4. Wait for PVC-1 to reach Bound state.
		5. Describe PV and verify node affinity details should contain both region-1 and site-1 details.
		6. Verify volume should be provisioned on the selected preferred datastore of site-1.
		7. Create Pod from PVC-1 created above.
		8. Wait for Pod to reach running state.
		9. Make sure Pod is running on the same node as mentioned in the node affinity details.
		10. Create Storage Class SC2 with Immedaite binding mode and allowed topologies set to region-1 and site-2.
		11. Create StatefulSet sts2 using default pod management policy with replica 1 using SC2
		12. Wait for StatefulSet sts2 to come up.
		13. Wait for PV, PVC to bound and POD to reach running state
		14. Describe PV and verify node affinity details should contain both region-1 and site-2 details.
		15. Verify volume should be provisioned on the selected preferred datastore of site-2.
		16. Make sure POD is running on the same node as mentioned in the node affinity details
		17. Perform cleanup. Delete StatefulSet, PVC, PV and SC.
		18. Remove datastore preference tags as part of cleanup.
	*/
	ginkgo.It("Tag single datastore and verify it is honored", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		topologyDomainForSite1 := GetAndExpectStringEnvVar(envTopologyDomainForSite1)
		regionValues, zoneValues, allowedTopologies = topologyParameterForStorageClass(topologyDomainForSite1)

		// ClusterdatastoreListMap = getDatastoreListByCluster(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
		// 	sshClientConfig, masterIp, clusters[0])

		// for dsKey := range ClusterdatastoreListMap {
		// 	choosePreferredDatastore(vcAddress, vCenterPort, vCenterUIUser, vCenterUIPassword,
		// 		sshClientConfig, masterIp, dsKey, zoneValues[0])
		// 	break
		// }

		storageclass1, pvclaim1, err := createPVCAndStorageClass(client,
			namespace, nil, nil, "", allowedTopologies, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim1.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleting the Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to pass provisioning volume")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim1.Namespace, pvclaim1.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified zone and region")
		pv1 := getPvFromClaim(client, pvclaim1.Namespace, pvclaim1.Name)
		if allowedTopologies == nil {
			zoneValues = allZones
			regionValues = allRegions
		}
		pvRegion, pvZone, err = verifyVolumeTopology(pv1, zoneValues, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a pod")
		pod1, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim1}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume:%s is attached to the node: %s",
			pv1.Spec.CSI.VolumeHandle, pod1.Spec.NodeName))
		vmUUID1 := getNodeUUID(ctx, client, pod1.Spec.NodeName)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv1.Spec.CSI.VolumeHandle, vmUUID1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By("Deleting the pod and wait for disk to detach")
			err := fpod.DeletePodWithWait(client, pod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
		err = verifyPodLocation(pod1, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		storageclass2, pvclaim2, err := createPVCAndStorageClass(client,
			namespace, nil, nil, "", allowedTopologies, bindingMode, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By("Deleting the PVC")
			err = fpv.DeletePersistentVolumeClaim(client, pvclaim2.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleting the Storage Class")
			err = client.StorageV1().StorageClasses().Delete(ctx, storageclass2.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to pass provisioning volume")
		err = fpv.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client,
			pvclaim2.Namespace, pvclaim2.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to provision volume with err: %v", err))

		ginkgo.By("Verify if volume is provisioned in specified zone and region")
		pv2 := getPvFromClaim(client, pvclaim2.Namespace, pvclaim2.Name)
		if allowedTopologies == nil {
			zoneValues = allZones
			regionValues = allRegions
		}
		pvRegion, pvZone, err = verifyVolumeTopology(pv2, zoneValues, regionValues)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating a pod")
		pod2, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim2}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume:%s is attached to the node: %s",
			pv2.Spec.CSI.VolumeHandle, pod2.Spec.NodeName))
		vmUUID2 := getNodeUUID(ctx, client, pod2.Spec.NodeName)
		isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, pv2.Spec.CSI.VolumeHandle, vmUUID2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")
		defer func() {
			ginkgo.By("Deleting the pod and wait for disk to detach")
			err := fpod.DeletePodWithWait(client, pod2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Verify Pod is scheduled on a node belonging to same topology as the PV it is attached to")
		err = verifyPodLocation(pod2, nodeList, pvZone, pvRegion)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})
