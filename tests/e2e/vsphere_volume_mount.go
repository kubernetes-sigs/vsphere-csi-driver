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

	"github.com/onsi/ginkgo/v2"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = ginkgo.Describe("[csi-file-vanilla] File Volume Attach Test", func() {
	f := framework.NewDefaultFramework("file-volume-attach-basic")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client    clientset.Interface
		namespace string
	)
	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

	})

	// Steps:
	// Test to Verify Pod can be created with PVC (dynamically provisioned) with
	// access mode "ReadWriteMany".
	// 1.Create a storage policy having "targetDatastoreUrls" as the compliant
	//   datastores.
	// 2.Create StorageClass with fsType as "nfs4" and storagePolicy created in
	//   Step 1.
	// 3.Create a PVC with "ReadWriteMany" using the SC from above.
	// 4.Wait for PVC to be Bound.
	// 5.Get the VolumeID from PV.
	// 6.Verify using CNS Query API if VolumeID retrieved from PV is present.
	//   Also verify Name, Capacity, VolumeType, Health matches.
	// 7.Verify if VolumeID is created on one of the VSAN datastores from list
	//   of datacenters provided in vsphere.conf.
	// 8.Create Pod1 using PVC created above at a mount path specified in PodSpec.
	// 9.Wait for Pod1 to be Running.
	// 10.Create a file (file1.txt) at the mount path. Check if the creation is
	//    successful.
	// 11.Delete Pod1.
	// 12.Verify if Pod1 is successfully deleted.
	// 13.Create Pod2 using PVC created above.
	// 14.Wait for Pod2 to be Running.
	// 15.Read the file (file1.txt) created in Step 8. Check if reading is
	//    successful.
	// 16.Create a new file (file2.txt) at the mount path. Check if the creation
	//    is successful.
	// 17.Delete Pod2.
	// 18.Verify if Pod2 is successfully deleted.
	// 19.Delete PVC.
	// 20.Check if the VolumeID is deleted from CNS by using CNSQuery API.
	// 21.Delete Storage Class.
	// 22.Delete storage policy.
	ginkgo.It("[cf-vanilla-file-f][csi-file-vanilla] Verify Pod can be created with PVC (dynamically provisioned) "+
		"with access mode ReadWriteMany", ginkgo.Label(p0, vanilla, file, core, vc70), func() {
		createFileVolumeAndMount(f, client, namespace)
	})
})
