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
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	testclient "k8s.io/client-go/kubernetes/fake"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

func TestStoragepool(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Storagepool Suite")
}

var (
	ctx       context.Context
	informer  metadataSyncInformer
	k8sClient *testclient.Clientset
)

var _ = BeforeSuite(func() {
	ctx = context.Background()

	config, _ := configFromEnvOrSim()

	config.Global.ClusterID = "test-cid"

	vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, config)
	Expect(err).To(BeNil())

	vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
	vcenter, err := vcManager.RegisterVirtualCenter(ctx, vcenterconfig)
	Expect(err).To(BeNil())

	vcenter.ConnectCns(ctx)

	getCandidateDatastores = getFakeDatastores

	// initialise storagepool service
	err = setUpInitStoragePoolService(ctx, vcenter, config.Global.ClusterID)
	Expect(err).To(BeNil())

	// initialise fake k8s client
	k8sClient = testclient.NewSimpleClientset()

	informer.k8sInformerManager = k8s.NewInformer(k8sClient)

})

var _ = AfterSuite(func() {

	// set function names back to the original ones after the tests have finished executing
	getCandidateDatastores = cnsvsphere.GetCandidateDatastoresInCluster
	findAccessibleNodesInCluster = findAccessibleNodes
	getDSProperties = getDatastoreProperties

})
