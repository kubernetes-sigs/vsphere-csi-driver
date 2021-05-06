/*
Copyright 2020 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

var _ = ginkgo.Describe("[vmc-gc] [csi-guest] Deploy Update and Scale Deployments", func() {
	f := framework.NewDefaultFramework("e2e-vsphere-deployment")
	var (
		namespace         string
		client            clientset.Interface
		storagePolicyName string
		scParameters      map[string]string
		deploymentName    string
		replica           int32
		maxReplica        int32
		pvclaims          []*v1.PersistentVolumeClaim
		pvclaim           *v1.PersistentVolumeClaim
		sc                *storagev1.StorageClass
	)
	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		namespace = getNamespaceToRunTests(f)
		client = f.ClientSet
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, storageclassname, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
	})

	ginkgo.AfterEach(func() {
		_, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Delete Resource Quota")
		deleteResourceQuota(client, namespace)

		if pvclaim != nil {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By(fmt.Sprintf("Deleting deployments in namespace: %v", namespace))
		fss.DeleteAllStatefulSets(client, namespace)

	})

	/*
		Test performs following operations

		Steps
		1. Deploy nginx pods with replica count as 3
		2. Update the nginx deployment pods, update the nginx image from k8s.gcr.io/nginx-slim:0.8 to k8s.gcr.io/nginx-slim:0.9
		3. Wait for some time and verify the update is successful
		4. Scale up the deployment to 10 replicas and verify all the pods should be up and running
		5. Scale down the deployment to 3 replicas and verify all the pods should be up and running
		6. Delete the deployments and Verify deployments are deleted successfully

	*/

	ginkgo.It("Scale up and scale down deployments", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		var lables = make(map[string]string)
		lables["app"] = "nginx"
		deploymentName = "testdeployment"
		replica = 3
		maxReplica = 10

		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName

		// create resource quota
		createResourceQuota(client, namespace, rqLimit, storageclassname)

		ginkgo.By("Creating StorageClass for Deployment")
		scSpec := getVSphereStorageClassSpec(storageclassname, scParameters, nil, "", "", false)
		sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating Deployments")
		deploymentSpec := NewDeployment(deploymentName, replica, lables, "nginx", "k8s.gcr.io/nginx-slim:0.8", "")
		createDeployments(namespace, deploymentSpec, client)

		ginkgo.By("Update deployment image")
		updateDeploymentImage(namespace, "k8s.gcr.io/nginx-slim:0.9", deploymentName)

		ginkgo.By("Scale up replicas")
		updateDeploymentReplica(client, maxReplica, deploymentName, namespace)
		dep, err := client.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("replica count after scale up of deployments %v", dep.Status.Replicas)
		gomega.Expect((dep.Status.Replicas) == maxReplica).To(gomega.BeTrue(), "Number of containers in the deployment should match with number of replicas")

		ginkgo.By("Scale down replicas")
		updateDeploymentReplica(client, replica, deploymentName, namespace)
		dep, err = client.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("replica count after scale down of deployments %v", dep.Status.Replicas)
		gomega.Expect((dep.Status.Replicas) == replica).To(gomega.BeTrue(), "Number of containers in the deployment should match with number of replicas")

		ginkgo.By("Delete Deployment")
		err = client.AppsV1().Deployments(namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/*
		Test performs following operations

		Steps
		1. Create Storage Class and PVC
		2. Deploy nginx pods with volume
		3. Update the nginx deployment pods, update the nginx image from k8s.gcr.io/nginx-slim:0.8 to k8s.gcr.io/nginx-slim:0.9
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
		deploymentName = "testdeployment"
		replica = 1
		maxReplica = 0

		ginkgo.By("CNS_TEST: Running for GC setup")

		// create resource quota
		createResourceQuota(client, namespace, rqLimit, storageclassname)

		ginkgo.By("Creating StorageClass for Deployment")
		scParameters[svStorageClassName] = storagePolicyName
		sc, pvclaim, err = createPVCAndStorageClass(client, namespace, nil, scParameters, "100Mi", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Creating PVC")
		pvclaims = append(pvclaims, pvclaim)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fpv.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim = nil
		}()

		ginkgo.By("Creating Deployments")
		deploymentSpec := NewDeploymentwithVolume(deploymentName, replica, lables, "nginx", "k8s.gcr.io/nginx-slim:0.8", pvclaims, "RollingUpdate")
		createDeployments(namespace, deploymentSpec, client)

		ginkgo.By("Update deployment image")
		updateDeploymentImage(namespace, "k8s.gcr.io/nginx-slim:0.9", deploymentName)

		ginkgo.By("Scale down replicas")
		updateDeploymentReplica(client, maxReplica, deploymentName, namespace)
		dep, err := client.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("replica count after scale down of deployments %v", dep.Status.Replicas)
		gomega.Expect((dep.Status.Replicas) == maxReplica).To(gomega.BeTrue(), "Number of containers in the deployment should match with number of replicas")

		ginkgo.By("Scale up replicas")
		updateDeploymentReplica(client, replica, deploymentName, namespace)
		dep, err = client.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("replica count after scale down of deployments %v", dep.Status.Replicas)
		gomega.Expect((dep.Status.Replicas) == replica).To(gomega.BeTrue(), "Number of containers in the deployment should match with number of replicas")

		ginkgo.By("Delete Deployment")
		err = client.AppsV1().Deployments(namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})
})
