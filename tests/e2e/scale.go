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
	"os"
	"strconv"
	"sync"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ = ginkgo.Describe("Scale Test", func() {
	f := framework.NewDefaultFramework("scale-test")
	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
		scParameters      map[string]string
	)

	ginkgo.BeforeEach(func() {
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		//Delete storage class if already present
		sc, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(
				ctx, sc.Name, *metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimitScaleTest)
		}

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}

		for _, pvc := range pvclaims {
			pvclaimToDelete, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
				ctx, pvc.Name, metav1.GetOptions{})
			if err == nil {
				err := fpv.DeletePersistentVolumeClaim(client, pvclaimToDelete.Name, pvclaimToDelete.Namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}

	})

	/*
		Test to Create and Delete PVC
		Steps
			1.	Create a Storage Class
			2.	Create PVC using above SC (pre data)
			3.	Create and Delete PVC (NUMBER_OF_GO_ROUTINES * WORKER_PER_ROUTINE) times
			4.	Delete SC
	*/
	ginkgo.It("[stress-supervisor] [stress-guest] Create and Delete PVC load", func() {
		var storageclass *storagev1.StorageClass
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Invoking Test to create and delete PVCs")

		// Getting the environment params to run the tests
		routine := os.Getenv(envNumberOfGoRoutines)
		if routine == "" {
			ginkgo.Skip("Env NUMBER_OF_GO_ROUTINES is missing")
		}
		goRoutine, err := strconv.Atoi(routine)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error Parsing "+routine)

		workerRoutine := os.Getenv(envWorkerPerRoutine)
		if workerRoutine == "" {
			ginkgo.Skip("Env WORKER_PER_ROUTINE is missing")
		}
		worker, err := strconv.Atoi(workerRoutine)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error Parsing "+workerRoutine)

		// decide which test setup is available to run
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID
			// create resource quota
			createResourceQuota(client, namespace, rqLimitScaleTest, storagePolicyName)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class with allowVolumeExpansion = true")

		storageclass, err = createStorageClass(client, scParameters, nil, "", "", true, storagePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		//Method to create few data
		var wg sync.WaitGroup
		lock := &sync.Mutex{}
		wg.Add(1)
		go scaleCreatePVC(client, namespace, nil, "", storageclass, "", &wg)
		wg.Wait()

		// create and delete PVC
		for i := 1; i <= goRoutine; i++ {
			wg.Add(1)
			go scaleCreateDeletePVC(client, namespace, nil, "", storageclass, "", &wg, lock, worker)
		}
		wg.Wait()

	})

})
