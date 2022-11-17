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

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[vmc-gc] Deploy, Update and Scale Deployments", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-deployment")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace         string
		client            clientset.Interface
		storagePolicyName string
		scParameters      map[string]string
		replica           int32
		pvclaims          []*v1.PersistentVolumeClaim
		pvclaim           *v1.PersistentVolumeClaim
		sc                *storagev1.StorageClass
		svNamespace       string
		volHandle         string
		deployment        *appsv1.Deployment
	)
	ginkgo.BeforeEach(func() {
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		bootstrap()

		ginkgo.By("Set Resource Quota")
		svcClient, svNamespace = getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, rqLimit)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ginkgo.By("Modify Resource Quota")
		svcClient, svNamespace := getSvcClientAndNamespace()
		setResourceQuota(svcClient, svNamespace, defaultrqLimit)

		if pvclaim != nil {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if deployment != nil {
			ginkgo.By("Delete Deployment")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*
		Test performs following operations

		Steps
		1. Create Storage Class and PVC
		2. Deploy nginx pods with volume
		3. Update the nginx deployment pods, update the nginx image from
		k8s.gcr.io/nginx-slim:0.8 to k8s.gcr.io/nginx-slim:0.9
		4. Wait for some time and verify the update is successful
		5. Scale dowm the deployment to 0 replicas
		6. Scale up the deployment to 1 replicas and verify all the pods should be up and running
		7. Delete the deployments and Verify deployments are deleted successfully
		8. Delete the PVC
	*/
	ginkgo.It("Deployments with volume provisioning", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		replica = 1

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		ginkgo.By("Creating StorageClass for Deployment")
		sc, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, diskSize, nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVC")
		pvclaims = append(pvclaims, pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle = persistentvolumes[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		ginkgo.By("Creating Deployments")
		deployment, err := createDeployment(ctx, client, replica, lables, nil, namespace, pvclaims, "", false, nginxImage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Updating the Deployment with max surge and max unavailable values")
		deployment.Spec.Strategy.RollingUpdate.MaxSurge = &intstr.IntOrString{Type: intstr.Int, IntVal: int32(0)}
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable = &intstr.IntOrString{Type: intstr.Int, IntVal: int32(1)}
		deployment, err = client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fdep.WaitForDeploymentComplete(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err := fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Update deployment image")
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deployment.Spec.Template.Spec.Containers[0].Image = nginxImage4upg
		deployment, err = fdep.UpdateDeploymentWithRetries(client, namespace, deployment.Name, deployment.DeepCopyInto)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = fdep.WaitForDeploymentComplete(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale down replicas")
		updateDeploymentReplica(client, replica-1, deployment.Name, namespace)

		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("replica count after scale down of deployments %v", deployment.Status.Replicas)
		gomega.Expect((deployment.Status.Replicas) == replica-1).To(gomega.BeTrue(),
			"Number of containers in the deployment should match with number of replicas")
		err = fpod.WaitForPodNotFoundInNamespace(client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Scale up replicas")
		updateDeploymentReplica(client, replica, deployment.Name, namespace)
		deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("replica count after scale up of deployments %v", deployment.Status.Replicas)
		gomega.Expect((deployment.Status.Replicas) == replica).To(gomega.BeTrue(),
			"Number of containers in the deployment should match with number of replicas")
		pods, err = fdep.GetPodsForDeployment(client, deployment)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod = pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete Deployment")
		err = client.AppsV1().Deployments(namespace).Delete(ctx, deployment.Name, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		deployment = nil

		ginkgo.By("Verify volume is detached from the node")
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
			volHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))

	})
})
