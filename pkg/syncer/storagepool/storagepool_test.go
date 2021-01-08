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

package storagepool

import (
	"context"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sigs.k8s.io/vsphere-csi-driver/pkg/apis/storagepool/cns/v1alpha1"
)

var (
	nodeAnnotation string = "vmware-system-esxi-node-moid"
)

var _ = Describe("Storagepool", func() {

	AfterEach(func() {

		// get SP with name expectedSpName
		expectedSpName := "storagepool-localds-0"
		spClient, spResource, err := getSPClient(ctx)
		Expect(err).To(BeNil())

		// Delete the SP so that the env is clean for the next test
		ctx := context.Background()
		spClient.Resource(*spResource).Delete(ctx, expectedSpName, metav1.DeleteOptions{})
	})

	Describe("when an SP doesn't exist", func() {
		It("should create SP", func() {
			findAccessibleNodesInCluster = findFakeAccessibleNodes
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			expectedSpName := "storagepool-localds-0"
			// SP with name expectedSpName must be created
			ctx := context.Background()
			_, err = spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})

			Expect(err).To(BeNil())

		})
	})

	Describe("when datastore values get updated", func() {
		It("SP values should get updated", func() {
			findAccessibleNodesInCluster = findFakeAccessibleNodes
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			expectedSpName := "storagepool-localds-0"
			// get SP with name expectedSpName
			ctx := context.Background()
			_, err = spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})

			// make sure SP was created initially
			Expect(err).To(BeNil())

			// use fake datastore properties to get updated DS values
			getDSProperties = getFakeDatastorePropertiesDSInMM
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			// get SP with name expectedSpName
			sp, _ := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})

			spCapacity, _, _ := unstructured.NestedMap(sp.Object, "status", "capacity")

			expectedFreeSpace, _ := strconv.ParseInt(freeSpaceStr, 10, 64)
			expectedTotalSpace, _ := strconv.ParseInt(capacityStr, 10, 64)

			// free and total space must be updated as per DS values
			Expect(spCapacity["freeSpace"]).To(Equal(expectedFreeSpace))
			Expect(spCapacity["total"]).To(Equal(expectedTotalSpace))

		})
	})

	Describe("when an invalid SP exists", func() {
		It("should delete that SP", func() {

			invalidSp := getStotagepoolInstance()

			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			// create invalid SP
			ctx := context.Background()
			_, err = spClient.Resource(*spResource).Create(ctx, invalidSp, metav1.CreateOptions{})
			// verify if the SP got created successfully
			Expect(err).To(BeNil())

			// Make sure SP with name fakeSPName actually got created
			expectedSpName := fakeSPName
			// get SP with name expectedSpName
			_, err = spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

			findAccessibleNodesInCluster = findFakeAccessibleNodes
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			// get SP with name expectedSpName
			_, err = spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})

			// error must not be nil as the SP got deleted
			Expect(err).NotTo(BeNil())

		})
	})

	Describe("when all hosts are in MM", func() {
		It("should contain the correct error message", func() {

			findAccessibleNodesInCluster = findFakeMMNodes
			getDSProperties = getDatastoreProperties
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			expectedSpName := "storagepool-localds-0"
			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			// get SP with name expectedSpName
			ctx := context.Background()
			sp, err := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

			spError, _, err := unstructured.NestedString(sp.Object, "status", "error", "message")
			Expect(err).To(BeNil())
			Expect(spError).To(Equal(v1alpha1.SpErrors[v1alpha1.ErrStateAllHostsInMM].Message))

		})
	})

	Describe("when all hosts are inaccessible", func() {
		It("should contain the correct error message", func() {

			findAccessibleNodesInCluster = findFakeInAccessibleNodes
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			expectedSpName := "storagepool-localds-0"
			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			// get SP with name expectedSpName
			ctx := context.Background()
			sp, err := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})

			Expect(err).To(BeNil())
			spError, _, err := unstructured.NestedString(sp.Object, "status", "error", "message")
			Expect(err).To(BeNil())
			Expect(spError).To(Equal(v1alpha1.SpErrors[v1alpha1.ErrStateNoAccessibleHosts].Message))

		})
	})

	Describe("when all hosts are in valid state", func() {
		It("should contain not error", func() {

			findAccessibleNodesInCluster = findFakeAccessibleNodes
			getDSProperties = getDatastoreProperties
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			expectedSpName := "storagepool-localds-0"
			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			// get SP with name expectedSpName
			ctx := context.Background()
			sp, err := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

			spError, _, err := unstructured.NestedString(sp.Object, "status", "error", "message")
			Expect(err).To(BeNil())
			Expect(spError).To(Equal(""))

		})
	})

	Describe("when datastores are in MM", func() {
		It("should contain the correct error message", func() {

			findAccessibleNodesInCluster = findFakeAccessibleNodes
			getDSProperties = getFakeDatastorePropertiesDSInMM
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			expectedSpName := "storagepool-localds-0"
			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			// get SP with name expectedSpName
			ctx := context.Background()
			sp, err := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

			spError, _, err := unstructured.NestedString(sp.Object, "status", "error", "message")
			Expect(err).To(BeNil())

			Expect(spError).To(Equal(v1alpha1.SpErrors[v1alpha1.ErrStateDatastoreInMM].Message))

		})
	})

	Describe("when datastores are inaccessible", func() {
		It("should contain the correct error message", func() {

			findAccessibleNodesInCluster = findFakeAccessibleNodes
			getDSProperties = getFakeDatastorePropertiesDSInaccessible
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			expectedSpName := "storagepool-localds-0"
			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			// get SP with name expectedSpName

			ctx := context.Background()
			sp, err := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

			spError, _, err := unstructured.NestedString(sp.Object, "status", "error", "message")
			Expect(err).To(BeNil())

			Expect(spError).To(Equal(v1alpha1.SpErrors[v1alpha1.ErrStateDatastoreNotAccessible].Message))

		})
	})

	Describe("when node is updated", func() {
		It("SP reconcilation occurs and SP is created", func() {
			findAccessibleNodesInCluster = findFakeAccessibleNodes

			go InitNodeAnnotationListener(ctx, informer.k8sInformerManager, scWatchCntlrTest, spControllerTest)

			time.Sleep(1000 * time.Millisecond)

			// Create a new node
			nodeAnn := make(map[string]string)
			nodeAnn[nodeAnnotation] = "host-123"
			nodeName := "wdc-node-2"
			node := getNodeSpec(nodeName, nodeAnn)
			ctx := context.Background()
			k8sClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})

			nodeAnn[nodeAnnotation] = "host-456"
			patchNode(nodeName, nodeAnn, k8sClient)

			time.Sleep(1000 * time.Millisecond)

			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			expectedSpName := "storagepool-localds-0"
			// get SP that is expected to be created
			_, err = spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

		})
	})

	Describe("when node is updated", func() {
		It("SP reconcilation occurs and SP values are updated", func() {
			findAccessibleNodesInCluster = findFakeAccessibleNodes
			// reconcile manually to make sure an inital SP is created
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			// Trigger NodeAnnotationListener
			go InitNodeAnnotationListener(ctx, informer.k8sInformerManager, scWatchCntlrTest, spControllerTest)

			// get fake datastore values that should be reflected in the SP created above
			getDSProperties = getFakeDatastorePropertiesDSInMM

			time.Sleep(1000 * time.Millisecond)

			nodeAnn := make(map[string]string)
			nodeAnn[nodeAnnotation] = "host-560"

			nodeName := "wdc-node-2"
			node := getNodeSpec(nodeName, nodeAnn)
			ctx := context.Background()
			k8sClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})

			// change node annotation to trigger reconcilation
			nodeAnn[nodeAnnotation] = "host-987"
			patchNode(nodeName, nodeAnn, k8sClient)

			time.Sleep(1000 * time.Millisecond)

			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			expectedSpName := "storagepool-localds-0"
			// Fetch SP that is expected to be created
			sp, err := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

			spCapacity, _, err := unstructured.NestedMap(sp.Object, "status", "capacity")
			Expect(err).To(BeNil())

			expectedFreeSpace, _ := strconv.ParseInt(freeSpaceStr, 10, 64)
			expectedTotalSpace, _ := strconv.ParseInt(capacityStr, 10, 64)

			// free and total space must be updated as per DS values
			Expect(spCapacity["freeSpace"]).To(Equal(expectedFreeSpace))
			Expect(spCapacity["total"]).To(Equal(expectedTotalSpace))

		})
	})

	Describe("when node is not updated", func() {
		It("SP should not be updated either", func() {
			findAccessibleNodesInCluster = findFakeAccessibleNodes
			getDSProperties = getDatastoreProperties
			// reconcile manually to make sure an inital SP is created
			ReconcileAllStoragePools(ctx, scWatchCntlrTest, spControllerTest)

			go InitNodeAnnotationListener(ctx, informer.k8sInformerManager, scWatchCntlrTest, spControllerTest)

			// get fake datastore values but they will not be reflected in the SP created above as the reconcilation is not expected to happen
			getDSProperties = getFakeDatastorePropertiesDSInMM

			time.Sleep(1000 * time.Millisecond)

			// create a new node
			nodeAnn := make(map[string]string)
			nodeAnn[nodeAnnotation] = "host-539"
			nodeName := "wdc-node-3"
			node := getNodeSpec(nodeName, nodeAnn)
			ctx := context.Background()
			k8sClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})

			spClient, spResource, err := getSPClient(ctx)
			Expect(err).To(BeNil())

			expectedSpName := "storagepool-localds-0"
			// get SP that is expected to be created
			sp, err := spClient.Resource(*spResource).Get(ctx, expectedSpName, metav1.GetOptions{})
			Expect(err).To(BeNil())

			spCapacity, _, err := unstructured.NestedMap(sp.Object, "status", "capacity")
			Expect(err).To(BeNil())

			expectedFreeSpace, _ := strconv.ParseInt(freeSpaceStr, 10, 64)
			expectedTotalSpace, _ := strconv.ParseInt(capacityStr, 10, 64)

			// free and total space must not be updated to fake DS values
			Expect(spCapacity["freeSpace"]).NotTo(Equal(expectedFreeSpace))
			Expect(spCapacity["total"]).NotTo(Equal(expectedTotalSpace))

		})
	})

})
