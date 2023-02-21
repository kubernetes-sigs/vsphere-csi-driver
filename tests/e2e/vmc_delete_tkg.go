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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Delete TKG", func() {
	f := framework.NewDefaultFramework("vmc-delete-tkg")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		svNamespace       string
		scParameters      map[string]string
		storagePolicyName string
		deleteGC          bool

		vmcUser string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		svcClient, svNamespace = getSvcClientAndNamespace()
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		setResourceQuota(svcClient, svNamespace, rqLimit)
		scParameters = make(map[string]string)
	})
	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
	})

	/*
		Test to Delete TKG on VMC
		Steps
			1. Create SC
			2. Create PVC with the above created SC and validate PVC is in bound phase
			3. Create POD with the above-created PVC and validate pod is in running state
			4. Delete the New Guest Cluster and validate the deleted Guest Cluster is not accessible
			5. Verify Volume Handle is available in SVC for the PVC created in the deleted GC
			6. Create PV in existing GC using the volume handle and validate it is in the available status
			7. Create PVC using the PV created in above step and validate it is in bound status
			8. Create POD validate it is in running status
			9. Verify CRD in SVC
			10. Delete POD, PVC, and PV in GC
	*/

	ginkgo.It("[delete-tkg-vmc] Delete TKG", func() {
		var err error
		var wcpToken string
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		deleteGC = false

		newGcKubconfigPath := os.Getenv("DELETE_GUEST_CLUSTER_KUBE_CONFIG")
		if newGcKubconfigPath == "" {
			ginkgo.Skip("Env DELETE_GUEST_CLUSTER_KUBE_CONFIG is missing")
		}

		vmcUser = os.Getenv("VMC_USER")
		if vmcUser == "" {
			ginkgo.Skip("Env VMC_USER is missing")
		}

		tkg_cluster := os.Getenv("TKG_CLUSTER_TO_DELETE")
		if tkg_cluster == "" {
			ginkgo.Skip("Env TKG_CLUSTER_TO_DELETE is missing")
		}

		clientNewGc, err := createKubernetesClientFromConfig(newGcKubconfigPath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			fmt.Sprintf("Error creating k8s client with %v: %v", newGcKubconfigPath, err))

		ginkgo.By("Creating namespace on GC2")
		ns, err := framework.CreateTestingNS(f.BaseName, clientNewGc, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on GC2")

		namespaceNewGC := ns.Name
		framework.Logf("Created namespace on GC2 %v", namespaceNewGC)
		defer func() {
			if !deleteGC {
				f.AddNamespacesToDelete(ns)
				framework.ExpectNoError(waitForNamespaceToGetDeleted(ctx,
					client, namespaceNewGC, poll, supervisorClusterOperationsTimeout))

			}
		}()

		ginkgo.By("Create PVC and POD in GC2")
		scParameters[scParamFsType] = ext4FSType
		// Create Storage class and PVC.
		ginkgo.By("Creating Storage Class")

		scParameters[svStorageClassName] = storagePolicyName
		storageclass, err := createStorageClass(clientNewGc, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err := createPVC(clientNewGc, namespaceNewGC, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Waiting for PVC to be bound.
		var pvclaims []*v1.PersistentVolumeClaim
		pvclaims = append(pvclaims, pvclaim)
		ginkgo.By("Waiting for all claims to be in bound state")
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(clientNewGc, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := persistentvolumes[0]
		volHandle := getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		svcPVCName := pv.Spec.CSI.VolumeHandle

		defer func() {
			if !deleteGC {
				err = clientNewGc.CoreV1().PersistentVolumeClaims(namespaceNewGC).Delete(ctx,
					pvclaim.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clientNewGc.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

			}
		}()

		ginkgo.By("Creating a pod in GC2")
		newPod, err := createPod(clientNewGc, namespaceNewGC, nil, pvclaims, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !deleteGC {
				err = fpod.DeletePodWithWait(clientNewGc, newPod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Verify volume is detached from the node")
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(clientNewGc,
					pv.Spec.CSI.VolumeHandle, newPod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, newPod.Spec.NodeName))

			}

		}()

		ginkgo.By("Delete TKG")

		if vmcUser == "testuser" {
			ginkgo.By("Get WCP session id")
			gomega.Expect((e2eVSphere.Config.Global.VmcDevopsUser)).NotTo(gomega.BeEmpty(), "Devops user is not set")
			wcpToken = getWCPSessionId(vmcWcpHost, e2eVSphere.Config.Global.VmcDevopsUser,
				e2eVSphere.Config.Global.VmcDevopsPassword)
			framework.Logf("vmcWcpHost %s", vmcWcpHost)
		} else {
			ginkgo.By("Get WCP session id")
			gomega.Expect((e2eVSphere.Config.Global.VmcCloudUser)).NotTo(gomega.BeEmpty(), "Cloud user is not set")
			framework.Logf("vmcWcpHost %s", vmcWcpHost)
			wcpToken = getWCPSessionId(vmcWcpHost, e2eVSphere.Config.Global.VmcCloudUser,
				e2eVSphere.Config.Global.VmcCloudPassword)
		}
		err = deleteTKG(vmcWcpHost, wcpToken, tkg_cluster)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deleteGC = true

		//validating static volume provisioning after GC2 deletion
		volHandle = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By("Creating namespace on GC1")
		ns, err = framework.CreateTestingNS(f.BaseName, client, map[string]string{
			"e2e-framework": f.BaseName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error creating namespace on GC1")

		gcNamespace := ns.Name
		framework.Logf("Created namespace on GC1 %v", gcNamespace)
		defer func() {
			f.AddNamespacesToDelete(ns)
			err := client.CoreV1().Namespaces().Delete(ctx, gcNamespace, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("creating PV in GC1")
		pvNew := getPersistentVolumeSpec(svcPVCName, v1.PersistentVolumeReclaimDelete, nil, ext4FSType)
		pvNew.Annotations = pv.Annotations
		pvNew.Spec.StorageClassName = pv.Spec.StorageClassName
		pvNew.Spec.CSI = pv.Spec.CSI
		pvNew.Spec.CSI.VolumeHandle = svcPVCName
		pvNew, err = client.CoreV1().PersistentVolumes().Create(ctx, pvNew, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create PVC and POD in GC1")
		pvcNew := getPersistentVolumeClaimSpec(gcNamespace, nil, pvNew.Name)
		pvcNew.Spec.StorageClassName = &pv.Spec.StorageClassName
		pvcNew, err = client.CoreV1().PersistentVolumeClaims(gcNamespace).Create(ctx, pvcNew, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Wait for PV and PVC to Bind.
		framework.ExpectNoError(fpv.WaitOnPVandPVC(client,
			framework.NewTimeoutContextWithDefaults(), gcNamespace, pvNew, pvcNew))

		defer func() {
			err = client.CoreV1().PersistentVolumeClaims(gcNamespace).Delete(ctx,
				pvcNew.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pvNew.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Create a new Pod to use this PVC, and verify volume has been attached.
		ginkgo.By("Creating a pod in GC1 with PVC created in GC1")
		pod, err := createPod(client, gcNamespace, nil, []*v1.PersistentVolumeClaim{pvcNew}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pvNew.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pvNew.Spec.CSI.VolumeHandle, pod.Spec.NodeName))

		}()

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pvNew.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(clientNewGc, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		//verify CRD in supervisor
		ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
		verifyCRDInSupervisorWithWait(ctx, f, pod.Spec.NodeName+"-"+svcPVCName,
			crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

	})

})
