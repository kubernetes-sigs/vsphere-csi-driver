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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-multi-vc-topology] Multi-VC", func() {
	f := framework.NewDefaultFramework("multi-vc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = f.Namespace.Name

		multiVCbootstrap()

		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

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

		framework.Logf("Perform cleanup of any left over stale PVs")
		allPvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for _, pv := range allPvs.Items {
			err := client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/* TESTCASE-1
			Deployment Pods and Standalone Pods creation with different SC. Perform Scaleup of Deployment Pods
	        SC1 → WFC Binding Mode with default values
			SC2 → Immediate Binding Mode with all allowed topologies i.e. zone-1 > zone-2 > zone-3

			Steps:
		    1. Create SC1 with default values so that all AZ's should be considered for volume provisioning.
		    2. Create PVC with "RWX" access mode using SC created in step #1.
		    3. Wait for PVC to reach Bound state
		    4. Create deployment Pod with replica count 3 and specify Node selector terms.
		    5. Volumes should get distributed across all AZs.
		    6. Pods should be running on the appropriate nodes as mentioned in Deployment node selector terms.
		    7. Verify CNS metadata for Pod, PVC and PV.
		    8. Create SC2 with Immediate Binding mode and with all levels of allowed topology set considering all 3 VC's.
		    9. Create PVC with "RWX" access mpde using SC created in step #8.
		    10. Verify PVC should reach to Bound state.
		    11. Create 5 Pods with different access mode and attach it to PVC created in step #9.
		    12. Verify Pods should reach to Running state.
		    13. Try reading/writing onto the volume from different Pods.
		    14. Perform scale up of deployment Pods to replica count 5. Verify Scaling operation should go smooth.
		    15. Perform cleanup by deleting deployment Pods, PVCs and the StorageClass (SC)
	*/

	ginkgo.It("qqqqqqq", ginkgo.Label(p0, block, vanilla, multiVc, newTest), func() {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = nfs4FSType
		replica := 3

		ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		storageclass1, pvclaim1, _, err := createRwxPvcWithStorageClass(client, namespace, labelsMap, scParameters, "", nil, "", false, v1.ReadWriteMany, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaims := append(pvclaims, pvclaim1)
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass1.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Deployments")
		deployment, err := createDeployment(ctx, client, int32(replica), labelsMap, nil, namespace, pvclaims, "", false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// ginkgo.By("Verify volume metadata for POD, PVC and PV")
		// err = waitAndVerifyCnsVolumeMetadata(pv1[0].Spec.CSI.VolumeHandle, pvclaim1, , pod)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By(fmt.Sprintf("Creating Storage Class with access mode %q and fstype %q", v1.ReadWriteMany, nfs4FSType))
		// storageclass, pvclaim, pv, err := createRwxPVCwithStorageClass(client, namespace, labelsMap, scParameters, "", nil, "", false, v1.ReadWriteMany)
		// gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// volHandle := pv[0].Spec.CSI.VolumeHandle
		// pvclaims := append(pvclaims, pvclaim)
		// defer func() {
		// 	err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		// 	gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// }()

	})

	/* TESTCASE-2
		StatefulSet Workload with default Pod Management and Scale-Up/Scale-Down Operations
		SC → Storage Policy (VC-1) with specific allowed topology i.e. k8s-zone:zone-1 and with WFC Binding mode

		Steps:
	    1. Create SC with storage policy name (tagged with vSAN ds) available in single VC (VC-1)
	    2. Create StatefulSet with 5 replicas using default Pod management policy
		3. Allow time for PVCs and Pods to reach Bound and Running states, respectively.
		4. Volume provisioning should happen on the AZ as specified in the SC.
		5. Pod placement can happen in any available availability zones (AZs)
		6. Verify CNS metadata for PVC and Pod.
		7. Scale-up/Scale-down the statefulset. Verify scaling operation went successful.
		8. Volume provisioning should happen on the AZ as specified in the SC but Pod placement can occur in any availability zones (AZs) during the scale-up operation.
		9. Perform cleanup by deleting StatefulSets, PVCs, and the StorageClass (SC)
	*/

	/* TESTCASE-3
			PVC and multiple Pods creation → Same Policy is available in two VCs
			SC → Same Storage Policy Name (VC-1 and VC-2) with specific allowed topology i.e. k8s-zone:zone-1,zone-2 and with Immediate Binding mode

			Steps:
	    1. Create SC with Storage policy name available in VC1 and VC2 and allowed topology set to k8s-zone:zone-1,zone-2 and with Immediate Binding mode
	    2. Create PVC with "RWX" access mode using SC created in step #1.
	    3. Create Deployment Pods with replica count 2 using PVC created in step #2.
	    4. Allow time for PVCs and Pods to reach Bound and Running states, respectively.
	    5. PVC should get created on the AZ as given in the SC.
	    6. Pod placement can happen on any AZ.
	    7. Verify CNS metadata for PVC and Pod.
	    8. Perform Scale up of deployment Pod replica to count 5
	    9. Verify scaling operation should go successful.
	    10. Perform scale down of deployment Pod to count 1.
	    11. Verify scaling down operation went smooth.
	    12. Perform cleanup by deleting Deployment, PVCs, and the SC
	*/

	/* TESTCASE-4
		Deployment Pod creation with allowed topology details in SC specific to VC-2
		SC → specific allowed topology i.e. k8s-zone:zone-2 and datastore url of VC-2 with WFC Binding mode

	Steps:
	1. Create SC with allowed topology of VC-2 and use datastore url from same VC with WFC Binding mode.
	2. Create PVC with "RWX" access mode using SC created in step #1.
	3. Verify PVC should reach to Bound state and it should be created on the AZ as given in the SC.
	4. Create deployment Pods with replica count 3 using PVC created in step #2.
	5. Wait for Pods to reach running ready state.
	6. Pods should get created on any VC or any AZ.
	7. Try reading/writing data into the volume.
	8. Perform cleanup by deleting deployment Pods, PVC and SC
	*/
})
