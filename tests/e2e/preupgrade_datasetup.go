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
	"math/rand"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"

	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

var _ = ginkgo.Describe("PreUpgrade datasetup Test", func() {

	f := framework.NewDefaultFramework("preupgrade-setup")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace         string
		client            clientset.Interface
		storageClassName  string
		pvclaim           *v1.PersistentVolumeClaim
		persistentvolumes []*v1.PersistentVolume
		podArray          []*v1.Pod
		labels_ns         map[string]string
		curtimestring     string
		val               string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		pvclaims = make([]*v1.PersistentVolumeClaim, 3)
		podArray = make([]*v1.Pod, 3)
		labels_ns = map[string]string{}
		labels_ns[admissionapi.EnforceLevelLabel] = string(admissionapi.LevelPrivileged)
		labels_ns["e2e-framework"] = f.BaseName

		// Generate a random value to aviod duplicate names for storage policy or pods or pvc
		curtime := time.Now().Unix()
		randomValue := rand.Int()
		val = strconv.FormatInt(int64(randomValue), 10)
		val = string(val[1:3])
		curtimestring = strconv.FormatInt(curtime, 10)
	})

	// Test to setup up predata before the Testbed is upgraded
	// Create sc, Stateful sets.

	// Steps
	// 1. Create a SC with allowVolumeExpansion set to 'true'
	// 2. create statefulset with replica 3 using the above created SC

	ginkgo.It("[csi-block-vanilla-preupgradedata] [csi-file-vanilla-preupgradedata] "+
		"Verify creation of statefulset", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// var pvcSizeBeforeExpansion int64
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		storageClassName = "preupgrade-sc-sts-" + curtimestring + val
		ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		scParameters[scParamDatastoreURL] = sharedVSANDatastoreURL

		namespace1, err := framework.CreateTestingNS(ctx, f.BaseName, client, labels_ns)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		namespace = namespace1.Name

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", true)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("storage class created is : %s", sc.Name)

		statefulset := GetStatefulSetFromManifest(namespace)
		ginkgo.By("Creating statefulset")
		statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
			Spec.StorageClassName = &storageClassName

		//For file Vanilla tests
		if rwxAccessMode {
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].Spec.AccessModes[0] =
				v1.ReadWriteMany
		}

		CreateStatefulSet(namespace, statefulset, client)
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")
	})

	// Test to setup up predata before the Testbed is upgraded
	// Create sc, multiple dynamic pvcs and attach them to pods .

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create PVC-1, PVC-2, PVC-3 which uses the StorageClass created in step 1.
	// 3. Wait for PV-1, PV-2, PV-3  to be provisioned.
	// 4. Wait for PVC's status to become Bound..
	// 6. Create pods using PVC's on specific node.
	// 7. Wait for Disk to be attached to the node.

	ginkgo.It("[csi-block-vanilla-preupgradedata] [csi-file-vanilla-preupgradedata] "+
		"Verify creation of dynamic pvcs and pods", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
		var err error
		storageClassName = "preupgrade-sc-pods-" + curtimestring + val
		namespace, err := framework.CreateTestingNS(ctx, f.BaseName, client, labels_ns)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", true)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("storage class created is : %s", sc.Name)

		count := 0
		for count < 3 {
			ginkgo.By("Creating PVCs using the Storage Class")

			accessMode := v1.ReadWriteOnce

			//For file Vanilla tests
			if rwxAccessMode {
				accessMode = v1.ReadWriteMany
			}

			pvclaims[count], err = fpv.CreatePVC(ctx, client, namespace.Name,
				getPersistentVolumeClaimSpecWithStorageClass(namespace.Name, diskSize, sc, nil, accessMode))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			count++
		}
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		podCount := 0
		for podCount < 3 {
			ginkgo.By("Creating pod to attach PVs to the node")
			pvclaim = pvclaims[podCount]
			podArray[podCount], err = createPod(ctx, client, namespace.Name, nil,
				[]*v1.PersistentVolumeClaim{pvclaim}, false, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			podCount++
		}

		//Skip for file Vanilla tests
		if !rwxAccessMode {
			var vmUUID string
			ginkgo.By("Verify the volumes are attached to the node vm")
			podCount = 0
			for podCount < 3 {
				pvclaim = pvclaims[podCount]
				pv := getPvFromClaim(client, namespace.Name, pvclaim.Name)

				volumeID := pv.Spec.CSI.VolumeHandle
				if vanillaCluster {
					vmUUID = getNodeUUID(ctx, client, podArray[podCount].Spec.NodeName)
				}
				ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
					pv.Spec.CSI.VolumeHandle, podArray[podCount].Spec.NodeName))
				isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume: %s is not attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, podArray[podCount].Spec.NodeName))
				podCount++
			}

			for _, pv := range persistentvolumes {

				framework.Logf("Volume: %s should be present in the CNS",
					pv.Spec.CSI.VolumeHandle)
				volumeID := pv.Spec.CSI.VolumeHandle
				err = e2eVSphere.waitForCNSVolumeToBeCreated(volumeID)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
		}
	})

	// Test to setup up predata before the Testbed is upgraded
	// Create sc, multiple dynamic standalone pvcs .

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create PVC-1, PVC-2, PVC-3 which uses the StorageClass created in step 1.
	// 3. Wait for PV-1, PV-2, PV-3  to be provisioned.
	// 4. Wait for PVC's status to become Bound.

	ginkgo.It("[csi-block-vanilla-preupgradedata] [csi-file-vanilla-preupgradedata] "+
		"Verify creation of standalone pvcs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
		var err error
		var pvclaimsarray = make([]*v1.PersistentVolumeClaim, 5)
		storageClassName = "preupgrade-sc-pvcs-" + curtimestring + val
		namespace, err := framework.CreateTestingNS(ctx, f.BaseName, client, labels_ns)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		scSpec := getVSphereStorageClassSpec(storageClassName, scParameters, nil, "", "", true)
		sc, err := client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("storage class created is : %s", sc.Name)

		accessMode := v1.ReadWriteOnce

		//For file Vanilla tests
		if rwxAccessMode {
			accessMode = v1.ReadWriteMany
		}

		for count := 0; count < 5; count++ {
			ginkgo.By("Creating PVCs using the Storage Class")
			pvclaimsarray[count], err = fpv.CreatePVC(ctx, client, namespace.Name,
				getPersistentVolumeClaimSpecWithStorageClass(namespace.Name, diskSize, sc, nil, accessMode))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsarray, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

})
