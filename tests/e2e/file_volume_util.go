package e2e

import (
	"context"
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

func createFileVolumeAndMount(f *framework.Framework, client clientset.Interface, namespace string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By(fmt.Sprintf("Invoking Test for accessMode: %s", v1.ReadWriteMany))
	sharedatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
	scParameters := make(map[string]string)
	scParameters[scParamDatastoreURL] = sharedatastoreURL
	const mntPath = "/mnt/volume1/"
	// Create Storage class and PVC.
	ginkgo.By("Creating Storage Class")
	storageclass, pvclaim, err := createPVCAndStorageClass(ctx, client,
		namespace, nil, scParameters, "", nil, "", false, v1.ReadWriteMany)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound.
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	volHandle := persistentvolumes[0].Spec.CSI.VolumeHandle

	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Verify using CNS Query API if VolumeID retrieved from PV is present.

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	ginkgo.By(fmt.Sprintf("volume Name:%s, capacity:%d volumeType:%s health:%s accesspoint: %s",
		queryResult.Volumes[0].Name,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb,
		queryResult.Volumes[0].VolumeType, queryResult.Volumes[0].HealthStatus,
		queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).AccessPoints),
	)

	// Verify if Volume capacity, name, type, health matches.
	ginkgo.By("Verifying disk size specified in PVC is honored")
	gomega.Expect(queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails).CapacityInMb ==
		diskSizeInMb).To(gomega.BeTrue(), "Wrong disk size provisioned")

	ginkgo.By("Verifying volume type specified in PVC is honored")
	gomega.Expect(queryResult.Volumes[0].VolumeType == testVolumeType).To(gomega.BeTrue(), "Volume type is not FILE")

	// Verify if VolumeID is created on the VSAN datastores.
	ginkgo.By("Verify if VolumeID is created on the VSAN datastores")
	gomega.Expect(strings.HasPrefix(queryResult.Volumes[0].DatastoreUrl, "ds:///vmfs/volumes/vsan:")).To(
		gomega.BeTrue(), "Volume is not provisioned on vSan datastore")

	// Verify if VolumeID is created on the datastore from list of datacenters
	// provided in vsphere.conf.
	ginkgo.By("Verify if VolumeID is created on the datastore from list of datacenters provided in vsphere.conf")
	gomega.Expect(isDatastoreBelongsToDatacenterSpecifiedInConfig(queryResult.Volumes[0].DatastoreUrl)).To(
		gomega.BeTrue(), "Volume is not provisioned on the datastore specified on config file")

	// Create Pod1 with pvc created above.
	ginkgo.By("Create Pod1 with pvc created above")
	pod1, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Cleanup for Pod1.
	defer func() {
		ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod1.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Create file1.txt on Pod1.
	filePath := mntPath + "file1.txt"
	ginkgo.By("Create file1.txt on Pod1")
	err = e2eoutput.CreateEmptyFileOnPod(namespace, pod1.Name, filePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Write data on file1.txt on Pod1.
	data := filePath
	ginkgo.By("Writing the file file1.txt from Pod1")

	_, err = e2ekubectl.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod1.Name,
		"--", "/bin/sh", "-c", fmt.Sprintf(" echo %s >  %s ", data, filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Read file1.txt created from Pod1.
	ginkgo.By("Read file1.txt from Pod1 created by Pod1")
	output, err := e2ekubectl.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod1.Name,
		"--", "/bin/sh", "-c", fmt.Sprintf("less %s", filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("File contents from file1.txt are: %s", output))
	gomega.Expect(output == data+"\n").To(gomega.BeTrue(), "Pod1 is able to read file1 written by Pod1")

	// Cleanup for Pod1.
	ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod1.Name, namespace))
	err = fpod.DeletePodWithWait(ctx, client, pod1)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	nodeName := pod1.Spec.NodeName
	ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from the node: %s", volHandle, nodeName))
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, nodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, nodeName))

	// Create Pod2 using the same pvc.
	ginkgo.By("Create Pod2 with pvc created above")
	pod2, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Cleanup for Pod2.
	defer func() {
		ginkgo.By(fmt.Sprintf("Deleting the pod : %s in namespace %s", pod2.Name, namespace))
		err = fpod.DeletePodWithWait(ctx, client, pod2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Read file1.txt created from Pod2.
	ginkgo.By("Read file1.txt from Pod2 created by Pod1")
	output, err = e2ekubectl.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod2.Name,
		"--", "/bin/sh", "-c", fmt.Sprintf("less %s", filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("File contents from file1.txt are: %s", output))
	gomega.Expect(output == data+"\n").To(gomega.BeTrue(), "Pod2 is able to read file1 written by Pod1")

	// Create a file file2.txt from Pod2.
	filePath = mntPath + "file2.txt"
	err = e2eoutput.CreateEmptyFileOnPod(namespace, pod2.Name, filePath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Write on file2.txt from Pod2")
	// Writing to the file.
	data = filePath
	_, err = e2ekubectl.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod2.Name,
		"--", "/bin/sh", "-c", fmt.Sprintf("echo %s >   %s", data, filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Read file2.txt created from Pod2.
	ginkgo.By("Read file2.txt from Pod2 created by Pod2")
	output, err = e2ekubectl.RunKubectl(namespace, "exec", fmt.Sprintf("--namespace=%s", namespace), pod2.Name,
		"--", "/bin/sh", "-c", fmt.Sprintf("less %s", filePath))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("File contents from file2.txt are: %s", output))
	gomega.Expect(output == data+"\n").To(gomega.BeTrue(), "Pod2 is able to read file2 written by Pod2")

	nodeName = pod2.Spec.NodeName
	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod2.Name, namespace))
	err = fpod.DeletePodWithWait(ctx, client, pod2)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from the node: %s", volHandle, nodeName))
	isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, nodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, nodeName))

}
