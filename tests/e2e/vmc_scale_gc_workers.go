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
	storagev1 "k8s.io/api/storage/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("Scale TKG Worker nodes", func() {
	f := framework.NewDefaultFramework("vmc-scale-tkg-worker-nodes")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client            clientset.Interface
		namespace         string
		svNamespace       string
		scParameters      map[string]string
		storagePolicyName string
		manifestPath      = "tests/e2e/testing-manifests/statefulset/nginx"
		vmcUser           string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		svcClient, svNamespace = getSvcClientAndNamespace()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		setResourceQuota(svcClient, svNamespace, rqLimit)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})
	ginkgo.AfterEach(func() {
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)
	})

	/*
		Test to Scale TKG worker nodes
		Steps
			1. Create 3 Statefulset pods, PVC with the above created SC and validate PVC is in bound phase
			2. Scale up the worker nodes to 5
			3. Scale up the Statefulset pods to 8
			4. Verify volumes are attached to the nodes
			5. Wait for some time to scale up operations to complete
			6. Verify PVC and POD should be in bound and running state
			7. Scale down the worker nodes to 2
			8. Wait for some time to scale down operations to complete
			9. Verify POD should be in running state
			10. Verify volumes are attached to the nodes
			11. Scale down the sts pods to 0
			12. Verify volumes are detached from the nodes
			10. Delete the statefulsets
	*/

	ginkgo.It("[vmc] Scale TKG Worker nodes", func() {
		var sc *storagev1.StorageClass
		var err error
		var wcpToken string
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vmcUser = os.Getenv("VMC_USER")
		if vmcUser == "" {
			ginkgo.Skip("Env vmcUser is missing")
		}

		ginkgo.By("Creating StorageClass for Statefulset")
		scParameters[svStorageClassName] = storagePolicyName
		scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
		sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating statefulset")
		statefulset := fss.CreateStatefulSet(ctx, client, manifestPath, namespace)
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting all statefulsets in namespace: %v", namespace))
			fss.DeleteAllStatefulSets(ctx, client, namespace)
			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Deleting service nginx in namespace: %v", namespace))
				err := client.CoreV1().Services(namespace).Delete(ctx, servicename, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()
		replicas := *(statefulset.Spec.Replicas)
		// Waiting for pods status to be Ready.
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsBeforeScaleUp := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsBeforeScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsBeforeScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Verify volumesa are attached to the nodes.
		for _, sspod := range ssPodsBeforeScaleUp.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
					volumeID := pv.Spec.CSI.VolumeHandle
					// svcPVCName refers to PVC Name in the supervisor cluster.
					svcPVCName := volumeID
					volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName,
						crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					verifyIsAttachedInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pv.Spec.CSI.VolumeHandle,
						crdVersion, crdGroup)

				}
			}
		}

		ginkgo.By("scaling up TKG worker node")
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

		scaleTKGWorker(vmcWcpHost, wcpToken, "test-cluster-e2e-script", 5)
		defer func() {
			if vmcUser == "testuser" {
				ginkgo.By("Get WCP session id")
				gomega.Expect((e2eVSphere.Config.Global.VmcDevopsUser)).NotTo(gomega.BeEmpty(), "Devops user is not set")
				wcpToken = getWCPSessionId(vmcWcpHost, e2eVSphere.Config.Global.VmcDevopsUser,
					e2eVSphere.Config.Global.VmcDevopsPassword)
				framework.Logf("vmcWcpHost %s", vmcWcpHost)
			} else {
				ginkgo.By("Get WCP session id")
				gomega.Expect((e2eVSphere.Config.Global.VmcCloudUser)).NotTo(gomega.BeEmpty(), "Devops user is not set")
				wcpToken = getWCPSessionId(vmcWcpHost, e2eVSphere.Config.Global.VmcCloudUser,
					e2eVSphere.Config.Global.VmcCloudPassword)
				framework.Logf("vmcWcpHost %s", vmcWcpHost)
				wcpToken = getWCPSessionId(vmcWcpHost, e2eVSphere.Config.Global.VmcCloudUser,
					e2eVSphere.Config.Global.VmcCloudPassword)
			}
			scaleTKGWorker(vmcWcpHost, wcpToken, "test-cluster-e2e-script", 2)
		}()

		err = getGC(vmcWcpHost, wcpToken, "test-cluster-e2e-script")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas+5))
		_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas+5)
		gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReplicas(ctx, client, statefulset, replicas+5)
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas+5)
		ssPodsAfterScaleUp := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas+5)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		// Verify volumesa are attached to the nodes.
		for _, sspod := range ssPodsAfterScaleUp.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
					volumeID := pv.Spec.CSI.VolumeHandle
					// svcPVCName refers to PVC Name in the supervisor cluster.
					svcPVCName := volumeID
					volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName,
						crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					verifyIsAttachedInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pv.Spec.CSI.VolumeHandle,
						crdVersion, crdGroup)

				}
			}
		}

		ginkgo.By("scaling down TKG worker node")
		scaleTKGWorker(vmcWcpHost, wcpToken, "test-cluster-e2e-script", 2)

		err = getGC(vmcWcpHost, wcpToken, "test-cluster-e2e-script")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas+5)
		gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
		ssPodsAfterScaleUp = fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
			fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
		gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas+5)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("check sts pods are up and running post scale down of tkg worker")
		// Verify volumesa are attached to the nodes.
		for _, sspod := range ssPodsAfterScaleUp.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volumespec := range sspod.Spec.Volumes {
				if volumespec.PersistentVolumeClaim != nil {
					pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
					ginkgo.By("Verify CnsNodeVmAttachment CRD is created")
					volumeID := pv.Spec.CSI.VolumeHandle
					// svcPVCName refers to PVC Name in the supervisor cluster.
					svcPVCName := volumeID
					volumeID = getVolumeIDFromSupervisorCluster(svcPVCName)
					gomega.Expect(volumeID).NotTo(gomega.BeEmpty())
					verifyCRDInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+svcPVCName,
						crdCNSNodeVMAttachment, crdVersion, crdGroup, true)

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					var vmUUID string
					vmUUID, err = getVMUUIDFromNodeName(sspod.Spec.NodeName)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volumeID, vmUUID)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
						fmt.Sprintf("Volume is not attached to the node, %s", vmUUID))

					ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
						pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
					verifyIsAttachedInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pv.Spec.CSI.VolumeHandle,
						crdVersion, crdGroup)

				}
			}
		}

		ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", 0))
		_, scaledownErr := fss.Scale(ctx, client, statefulset, 0)
		gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
		fss.WaitForStatusReadyReplicas(ctx, client, statefulset, 0)
		ssPodsAfterScaleDown := fss.GetPodList(ctx, client, statefulset)
		gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(0)).To(gomega.BeTrue(),
			"Number of Pods in the statefulset should match with number of replicas")

		ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
		for _, sspod := range ssPodsAfterScaleUp.Items {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
			if err != nil {
				gomega.Expect(apierrs.IsNotFound(err), gomega.BeTrue())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						verifyIsDetachedInSupervisor(ctx, f, sspod.Spec.NodeName+"-"+pv.Spec.CSI.VolumeHandle,
							crdVersion, crdGroup)
					}
				}
			}
		}

	})

})
