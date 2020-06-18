/*
Copyright 2019 The Kubernetes Authors.

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
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*
	Test to verify sVmotion works fine for volumes in detached state

	Steps
	1. Create StorageClass.
	2. Create PVC.
	3. Expect PVC to pass and verified it is created correctly.
	4. Relocate detached volume
	5. Invoke CNS Query API and validate datastore URL value changed correctly
*/

var _ = ginkgo.Describe("[csi-block-vanilla] Relocate detached volume ", func() {
	f := framework.NewDefaultFramework("svmotion-detached-disk")
	var (
		client          clientset.Interface
		namespace       string
		scParameters    map[string]string
		datastoreURL    string
		sourceDatastore *object.Datastore
		destDatastore   *object.Datastore
		datacenter      *object.Datacenter
		destDsURL       string
		pvclaims        []*v1.PersistentVolumeClaim
		fcdID           string
	)
	ginkgo.BeforeEach(func() {
		bootstrap()
		client = f.ClientSet
		namespace = f.Namespace.Name
		scParameters = make(map[string]string)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		destDsURL = GetAndExpectStringEnvVar(destinationDatastoreURL)
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	// Test for relocating volume being detached state
	ginkgo.It("Verify relocating detached volume works fine", func() {
		ginkgo.By("Invoking Test for relocating detached volume")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		var datacenters []string
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters,
			",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}

		for _, dc := range datacenters {
			datacenter, err = finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(datacenter)
			sourceDatastore, err = getDatastoreByURL(ctx, datastoreURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			destDatastore, err = getDatastoreByURL(ctx, destDsURL, datacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		scParameters[scParamDatastoreURL] = datastoreURL
		storageclass, pvclaim, err := createPVCAndStorageClass(client, namespace, nil, scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			err = client.StorageV1().StorageClasses().Delete(storageclass.Name, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = framework.DeletePersistentVolumeClaim(client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Expect claim to provision volume successfully")
		err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to provision volume")

		pvclaims = append(pvclaims, pvclaim)

		persistentvolumes, err := framework.WaitForPVClaimBoundPhase(client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		fcdID = persistentvolumes[0].Spec.CSI.VolumeHandle

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", persistentvolumes[0].Spec.CSI.VolumeHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(persistentvolumes[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("Error: QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk is created on the specified datastore")
		if queryResult.Volumes[0].DatastoreUrl != datastoreURL {
			err = fmt.Errorf("Disk is created on the wrong datastore")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Relocate volume
		err = e2eVSphere.relocateFCD(ctx, fcdID, sourceDatastore.Reference(), destDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to finish FCD relocation:%s to sync with pandora", defaultPandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(defaultPandoraSyncWaitTime) * time.Second)

		// verify disk is relocated to the specified destination datastore
		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s after relocating the disk", persistentvolumes[0].Spec.CSI.VolumeHandle))
		queryResult, err = e2eVSphere.queryCNSVolumeWithResult(persistentvolumes[0].Spec.CSI.VolumeHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("Error: QueryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk is relocated to the specified datastore")
		if queryResult.Volumes[0].DatastoreUrl != destDsURL {
			err = fmt.Errorf("Disk is relocated on the wrong datastore")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
