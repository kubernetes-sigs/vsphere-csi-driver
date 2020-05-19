package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/manifest"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsnodevmattachment/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvolumemetadata/v1alpha1"
)

// getVSphereStorageClassSpec returns Storage Class Spec with supplied storage class parameters
func getVSphereStorageClassSpec(scName string, scParameters map[string]string, allowedTopologies []v1.TopologySelectorLabelRequirement, scReclaimPolicy v1.PersistentVolumeReclaimPolicy, bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool) *storagev1.StorageClass {
	if bindingMode == "" {
		bindingMode = storagev1.VolumeBindingImmediate
	}
	var sc = &storagev1.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "sc-",
		},
		Provisioner:          e2evSphereCSIBlockDriverName,
		VolumeBindingMode:    &bindingMode,
		AllowVolumeExpansion: &allowVolumeExpansion,
	}
	// If scName is specified, use that name, else auto-generate storage class name
	if scName != "" {
		sc.ObjectMeta.Name = scName
	}
	if scParameters != nil {
		sc.Parameters = scParameters
	}
	if allowedTopologies != nil {
		sc.AllowedTopologies = []v1.TopologySelectorTerm{
			{
				MatchLabelExpressions: allowedTopologies,
			},
		}
	}
	if scReclaimPolicy != "" {
		sc.ReclaimPolicy = &scReclaimPolicy
	}

	return sc
}

// getPvFromClaim returns PersistentVolume for requested claim
func getPvFromClaim(client clientset.Interface, namespace string, claimName string) *v1.PersistentVolume {
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(claimName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv, err := client.CoreV1().PersistentVolumes().Get(pvclaim.Spec.VolumeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return pv
}

// getNodeUUID returns Node VM UUID for requested node
func getNodeUUID(client clientset.Interface, nodeName string) string {
	node, err := client.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vmUUID := strings.TrimPrefix(node.Spec.ProviderID, providerPrefix)
	gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
	ginkgo.By(fmt.Sprintf("VM UUID is: %s for node: %s", vmUUID, nodeName))
	return vmUUID
}

// getVMUUIDFromNodeName returns the vmUUID for a given node vm and datacenter
func getVMUUIDFromNodeName(nodeName string) (string, error) {
	var datacenters []string
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dcList := strings.Split(cfg.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}
	var vm *object.VirtualMachine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, dc := range datacenters {
		dataCenter, err := finder.Datacenter(ctx, dc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		finder := find.NewFinder(dataCenter.Client(), false)
		finder.SetDatacenter(dataCenter)
		vm, err = finder.VirtualMachine(ctx, nodeName)
		if err != nil {
			continue
		}
		vmUUID := vm.UUID(ctx)
		gomega.Expect(vmUUID).NotTo(gomega.BeEmpty())
		ginkgo.By(fmt.Sprintf("VM UUID is: %s for node: %s", vmUUID, nodeName))
		return vmUUID, nil
	}
	return "", err
}

// verifyVolumeMetadataInCNS verifies container volume metadata is matching the one is CNS cache
func verifyVolumeMetadataInCNS(vs *vSphere, volumeID string, PersistentVolumeClaimName string, PersistentVolumeName string,
	PodName string, Labels ...types.KeyValue) error {
	queryResult, err := vs.queryCNSVolumeWithResult(volumeID)
	if err != nil {
		return err
	}
	gomega.Expect(queryResult.Volumes).ShouldNot(gomega.BeEmpty())
	if len(queryResult.Volumes) != 1 || queryResult.Volumes[0].VolumeId.Id != volumeID {
		return fmt.Errorf("failed to query cns volume %s", volumeID)
	}
	for _, metadata := range queryResult.Volumes[0].Metadata.EntityMetadata {
		kubernetesMetadata := metadata.(*cnstypes.CnsKubernetesEntityMetadata)
		if kubernetesMetadata.EntityType == "POD" && kubernetesMetadata.EntityName != PodName {
			return fmt.Errorf("entity POD with name %s not found for volume %s", PodName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME" && kubernetesMetadata.EntityName != PersistentVolumeName {
			return fmt.Errorf("entity PV with name %s not found for volume %s", PersistentVolumeName, volumeID)
		} else if kubernetesMetadata.EntityType == "PERSISTENT_VOLUME_CLAIM" && kubernetesMetadata.EntityName != PersistentVolumeClaimName {
			return fmt.Errorf("entity PVC with name %s not found for volume %s", PersistentVolumeClaimName, volumeID)
		}
	}
	labelMap := make(map[string]string)
	for _, e := range queryResult.Volumes[0].Metadata.EntityMetadata {
		if e == nil {
			continue
		}
		if e.GetCnsEntityMetadata().Labels == nil {
			continue
		}
		for _, al := range e.GetCnsEntityMetadata().Labels {
			// these are the actual labels in the provisioned PV. populate them in the label map
			labelMap[al.Key] = al.Value
		}
		for _, el := range Labels {
			// Traverse through the slice of expected labels and see if all of them are present in the label map
			if val, ok := labelMap[el.Key]; ok {
				gomega.Expect(el.Value == val).To(gomega.BeTrue(),
					fmt.Sprintf("Actual label Value of the statically provisioned PV is %s but expected is %s", val, el.Value))
			} else {
				return fmt.Errorf("label(%s:%s) is expected in the provisioned PV but its not found", el.Key, el.Value)
			}
		}
	}
	ginkgo.By(fmt.Sprintf("successfully verified metadata of the volume %q", volumeID))
	return nil
}

// getVirtualDeviceByDiskID gets the virtual device by diskID
func getVirtualDeviceByDiskID(ctx context.Context, vm *object.VirtualMachine, diskID string) (vim25types.BaseVirtualDevice, error) {
	vmname, err := vm.Common.ObjectName(ctx)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		framework.Logf("failed to get the devices for VM: %q. err: %+v", vmname, err)
		return nil, err
	}
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			if virtualDisk, ok := device.(*vim25types.VirtualDisk); ok {
				if virtualDisk.VDiskId != nil && virtualDisk.VDiskId.Id == diskID {
					framework.Logf("Found FCDID %q attached to VM %q", diskID, vmname)
					return device, nil
				}
			}
		}
	}
	framework.Logf("failed to find FCDID %q attached to VM %q", diskID, vmname)
	return nil, nil
}

// getPersistentVolumeClaimSpecWithStorageClass return the PersistentVolumeClaim spec with specified storage class
func getPersistentVolumeClaimSpecWithStorageClass(namespace string, ds string, storageclass *storagev1.StorageClass, pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	disksize := diskSize
	if ds != "" {
		disksize = ds
	}
	if accessMode == "" {
		// if accessMode is not specified, set the default accessMode
		accessMode = v1.ReadWriteOnce
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(disksize),
				},
			},
			StorageClassName: &(storageclass.Name),
		},
	}

	if pvclaimlabels != nil {
		claim.Labels = pvclaimlabels
	}

	return claim
}

// createPVCAndStorageClass helps creates a storage class with specified name, storageclass parameters and PVC using storage class
func createPVCAndStorageClass(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string, scParameters map[string]string, ds string,
	allowedTopologies []v1.TopologySelectorLabelRequirement, bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool, accessMode v1.PersistentVolumeAccessMode, names ...string) (*storagev1.StorageClass, *v1.PersistentVolumeClaim, error) {
	scName := ""
	if len(names) > 0 {
		scName = names[0]
	}
	storageclass, err := createStorageClass(client, scParameters, allowedTopologies, "", bindingMode, allowVolumeExpansion, scName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvclaim, err := createPVC(client, pvcnamespace, pvclaimlabels, ds, storageclass, accessMode)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return storageclass, pvclaim, err
}

// createStorageClass helps creates a storage class with specified name, storageclass parameters
func createStorageClass(client clientset.Interface, scParameters map[string]string, allowedTopologies []v1.TopologySelectorLabelRequirement,
	scReclaimPolicy v1.PersistentVolumeReclaimPolicy, bindingMode storagev1.VolumeBindingMode, allowVolumeExpansion bool, scName string) (*storagev1.StorageClass, error) {
	ginkgo.By(fmt.Sprintf("Creating StorageClass [%q] With scParameters: %+v and allowedTopologies: %+v and ReclaimPolicy: %+v and allowVolumeExpansion: %t", scName, scParameters, allowedTopologies, scReclaimPolicy, allowVolumeExpansion))
	storageclass, err := client.StorageV1().StorageClasses().Create(getVSphereStorageClassSpec(scName, scParameters, allowedTopologies, scReclaimPolicy, bindingMode, allowVolumeExpansion))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to create storage class with err: %v", err))
	return storageclass, err
}

// createPVC helps creates pvc with given namespace and labels using given storage class
func createPVC(client clientset.Interface, pvcnamespace string, pvclaimlabels map[string]string, ds string, storageclass *storagev1.StorageClass, accessMode v1.PersistentVolumeAccessMode) (*v1.PersistentVolumeClaim, error) {
	pvcspec := getPersistentVolumeClaimSpecWithStorageClass(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode)
	ginkgo.By(fmt.Sprintf("Creating PVC using the Storage Class %s with disk size %s and labels: %+v accessMode: %+v", storageclass.Name, ds, pvclaimlabels, accessMode))
	pvclaim, err := framework.CreatePVC(client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("failed to create pvc with err: %v", err))
	return pvclaim, err
}

// createStatefulSetWithOneReplica helps create a stateful set with one replica
func createStatefulSetWithOneReplica(client clientset.Interface, manifestPath string, namespace string) *appsv1.StatefulSet {
	mkpath := func(file string) string {
		return filepath.Join(manifestPath, file)
	}
	statefulSet, err := manifest.StatefulSetFromManifest(mkpath("statefulset.yaml"), namespace)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	service, err := manifest.SvcFromManifest(mkpath("service.yaml"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = client.CoreV1().Services(namespace).Create(service)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*statefulSet.Spec.Replicas = 1
	_, err = client.AppsV1().StatefulSets(namespace).Create(statefulSet)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return statefulSet
}

// updateDeploymentReplica helps to update the replica for a deployment
func updateDeploymentReplica(client clientset.Interface, count int32, name string, namespace string) *appsv1.Deployment {
	deployment, err := client.AppsV1().Deployments(namespace).Get(name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	*deployment.Spec.Replicas = count
	deployment, err = client.AppsV1().Deployments(namespace).Update(deployment)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Waiting for update operation on deployment to take effect")
	time.Sleep(1 * time.Minute)
	return deployment
}

// getLabelsMapFromKeyValue returns map[string]string for given array of vim25types.KeyValue
func getLabelsMapFromKeyValue(labels []vim25types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// getDatastoreByURL returns the *Datastore instance given its URL.
func getDatastoreByURL(ctx context.Context, datastoreURL string, dc *object.Datacenter) (*object.Datastore, error) {
	finder := find.NewFinder(dc.Client(), false)
	finder.SetDatacenter(dc)
	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		framework.Logf("failed to get all the datastores. err: %+v", err)
		return nil, err
	}
	var dsList []types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}

	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(dc.Client())
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		framework.Logf("failed to get Datastore managed objects from datastore objects."+
			" dsObjList: %+v, properties: %+v, err: %v", dsList, properties, err)
		return nil, err
	}
	for _, dsMo := range dsMoList {
		if dsMo.Info.GetDatastoreInfo().Url == datastoreURL {
			return object.NewDatastore(dc.Client(),
				dsMo.Reference()), nil
		}
	}
	err = fmt.Errorf("Couldn't find Datastore given URL %q", datastoreURL)
	return nil, err
}

// getPersistentVolumeClaimSpec gets vsphere persistent volume spec with given selector labels
// and binds it to given pv
func getPersistentVolumeClaimSpec(namespace string, labels map[string]string, pvName string) *v1.PersistentVolumeClaim {
	var (
		pvc *v1.PersistentVolumeClaim
	)
	sc := ""
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
	}
	if labels != nil {
		pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}

	return pvc
}

// function to create PV volume spec with given FCD ID, Reclaim Policy and labels
func getPersistentVolumeSpec(fcdID string, persistentVolumeReclaimPolicy v1.PersistentVolumeReclaimPolicy, labels map[string]string) *v1.PersistentVolume {
	var (
		pvConfig framework.PersistentVolumeConfig
		pv       *v1.PersistentVolume
		claimRef *v1.ObjectReference
	)
	pvConfig = framework.PersistentVolumeConfig{
		NamePrefix: "vspherepv-",
		PVSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       e2evSphereCSIBlockDriverName,
				VolumeHandle: fcdID,
				ReadOnly:     false,
				FSType:       "ext4",
			},
		},
		Prebind: nil,
	}

	pv = &v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvConfig.NamePrefix,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: persistentVolumeReclaimPolicy,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
			},
			PersistentVolumeSource: pvConfig.PVSource,
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			ClaimRef:         claimRef,
			StorageClassName: "",
		},
		Status: v1.PersistentVolumeStatus{},
	}
	if labels != nil {
		pv.Labels = labels
	}
	// Annotation needed to delete a statically created pv
	annotations := make(map[string]string)
	annotations["pv.kubernetes.io/provisioned-by"] = e2evSphereCSIBlockDriverName
	pv.Annotations = annotations
	return pv
}

// invokeVCenterServiceControl invokes the given command for the given service
// via service-control on the given vCenter host over SSH.
func invokeVCenterServiceControl(command, service, host string) error {
	sshCmd := fmt.Sprintf("service-control --%s %s", command, service)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := framework.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		framework.LogSSHResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	return nil
}

// writeToFile will take two parameters:
// 1. the absolute path of the file(including filename) to be created
// 2. data content to be written into the file
// Returns nil on Success and error on failure
func writeToFile(filePath, data string) error {
	if filePath == "" {
		return fmt.Errorf("invalid filename")
	}
	f, err := os.Create(filePath)
	if err != nil {
		framework.Logf("Error: %v", err)
		return err
	}
	_, err = f.WriteString(data)
	if err != nil {
		framework.Logf("Error: %v", err)
		f.Close()
		return err
	}
	err = f.Close()
	if err != nil {
		framework.Logf("Error: %v", err)
		return err
	}
	return nil
}

// invokeVCenterChangePassword invokes `dir-cli password reset` command on the given vCenter host over SSH
// thereby resetting the currentPassword of the `user` to the `newPassword`
func invokeVCenterChangePassword(user, adminPassword, newPassword, host string) error {
	// create an input file and write passwords into it
	path := "input.txt"
	data := fmt.Sprintf("%s\n%s\n", adminPassword, newPassword)
	err := writeToFile(path, data)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		// delete the input file containing passwords
		err = os.Remove(path)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	// remote copy this input file to VC
	copyCmd := fmt.Sprintf("/bin/cat %s | /usr/bin/ssh root@%s '/usr/bin/cat >> input_copy.txt'", path, e2eVSphere.Config.Global.VCenterHostname)
	fmt.Printf("Executing the command: %s\n", copyCmd)
	_, err = exec.Command("/bin/sh", "-c", copyCmd).Output()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		// remove the input_copy.txt file from VC
		removeCmd := fmt.Sprintf("/usr/bin/ssh root@%s '/usr/bin/rm input_copy.txt'", e2eVSphere.Config.Global.VCenterHostname)
		_, err = exec.Command("/bin/sh", "-c", removeCmd).Output()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	sshCmd := fmt.Sprintf("/usr/bin/cat input_copy.txt | /usr/lib/vmware-vmafd/bin/dir-cli password reset --account %s", user)
	framework.Logf("Invoking command %v on vCenter host %v", sshCmd, host)
	result, err := framework.SSH(sshCmd, host, framework.TestContext.Provider)
	if err != nil || result.Code != 0 {
		framework.LogSSHResult(result)
		return fmt.Errorf("couldn't execute command: %s on vCenter host: %v", sshCmd, err)
	}
	if !strings.Contains(result.Stdout, "Password was reset successfully for ") {
		framework.Logf("failed to change the password for user %s: %s", user, result.Stdout)
		return err
	}
	framework.Logf("password changed successfully for user: %s", user)
	return nil
}

// verifyVolumeTopology verifies that the Node Affinity rules in the volume
// match the topology constraints specified in the storage class
func verifyVolumeTopology(pv *v1.PersistentVolume, zoneValues []string, regionValues []string) (string, string, error) {
	if pv.Spec.NodeAffinity == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return "", "", fmt.Errorf("Node Affinity rules for PV should exist in topology aware provisioning")
	}
	var pvZone string
	var pvRegion string
	for _, labels := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions {
		if labels.Key == zoneKey {
			for _, value := range labels.Values {
				gomega.Expect(zoneValues).To(gomega.ContainElement(value), fmt.Sprintf("Node Affinity rules for PV %s: %v does not contain zone specified in storage class %v", pv.Name, value, zoneValues))
				pvZone = value
			}
		}
		if labels.Key == regionKey {
			for _, value := range labels.Values {
				gomega.Expect(regionValues).To(gomega.ContainElement(value), fmt.Sprintf("Node Affinity rules for PV %s: %v does not contain region specified in storage class %v", pv.Name, value, regionValues))
				pvRegion = value
			}
		}
	}
	framework.Logf("PV %s is located in zone: %s and region: %s", pv.Name, pvZone, pvRegion)
	return pvRegion, pvZone, nil
}

// verifyPodLocation verifies that a pod is scheduled on
// a node that belongs to the topology on which PV is provisioned
func verifyPodLocation(pod *v1.Pod, nodeList *v1.NodeList, zoneValue string, regionValue string) error {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			for labelKey, labelValue := range node.Labels {
				if labelKey == zoneKey && zoneValue != "" {
					gomega.Expect(zoneValue).To(gomega.Equal(labelValue), fmt.Sprintf("Pod %s is not running on Node located in zone %v", pod.Name, zoneValue))
				}
				if labelKey == regionKey && regionValue != "" {
					gomega.Expect(regionValue).To(gomega.Equal(labelValue), fmt.Sprintf("Pod %s is not running on Node located in region %v", pod.Name, regionValue))
				}
			}
		}
	}
	return nil
}

// getTopologyFromPod rturn topology value from node affinity information
func getTopologyFromPod(pod *v1.Pod, nodeList *v1.NodeList) (string, string, error) {
	for _, node := range nodeList.Items {
		if pod.Spec.NodeName == node.Name {
			podRegion := node.Labels[v1.LabelZoneRegion]
			podZone := node.Labels[v1.LabelZoneFailureDomain]
			return podRegion, podZone, nil
		}
	}
	err := errors.New("Could not find the topology from pod")
	return "", "", err
}

// topologyParameterForStorageClass creates a topology map using the topology values ENV variables
// Returns the allowedTopologies parameters required for the Storage Class
// Input : <region-1>:<zone-1>, <region-1>:<zone-2>
// Output : [region-1], [zone-1, zone-2] {region-1: zone-1, region-1:zone-2}
func topologyParameterForStorageClass(topology string) ([]string, []string, []v1.TopologySelectorLabelRequirement) {
	topologyMap := createTopologyMap(topology)
	regionValues, zoneValues := getValidTopology(topologyMap)
	allowedTopologies := []v1.TopologySelectorLabelRequirement{
		{
			Key:    regionKey,
			Values: regionValues,
		},
		{
			Key:    zoneKey,
			Values: zoneValues,
		},
	}
	return regionValues, zoneValues, allowedTopologies
}

// createTopologyMap strips the topology string provided in the environment variable
// into a map from region to zones
// example envTopology = "r1:z1,r1:z2,r2:z3"
func createTopologyMap(topologyString string) map[string][]string {
	topologyMap := make(map[string][]string)
	for _, t := range strings.Split(topologyString, ",") {
		t = strings.TrimSpace(t)
		topology := strings.Split(t, ":")
		if len(topology) != 2 {
			continue
		}
		topologyMap[topology[0]] = append(topologyMap[topology[0]], topology[1])
	}
	return topologyMap
}

// getValidTopology returns the regions and zones from the input topology map
// so that they can be provided to a storage class
func getValidTopology(topologyMap map[string][]string) ([]string, []string) {
	var regionValues []string
	var zoneValues []string
	for region, zones := range topologyMap {
		regionValues = append(regionValues, region)
		zoneValues = append(zoneValues, zones...)
	}
	return regionValues, zoneValues
}

// createResourceQuota creates resource quota for the specified namespace.
func createResourceQuota(client clientset.Interface, namespace string, size string, scName string) {
	waitTime := 10
	resourceQuota := newTestResourceQuota(quotaName, size, scName)
	resourceQuota, err := client.CoreV1().ResourceQuotas(namespace).Create(resourceQuota)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By(fmt.Sprintf("Create Resource quota: %+v", resourceQuota))
	ginkgo.By(fmt.Sprintf("Waiting for %v seconds to allow resourceQuota to be claimed", waitTime))
	time.Sleep(time.Duration(waitTime) * time.Second)
}

// deleteResourceQuota deletes resource quota for the specified namespace, if it exists.
func deleteResourceQuota(client clientset.Interface, namespace string) {
	_, err := client.CoreV1().ResourceQuotas(namespace).Get(quotaName, metav1.GetOptions{})
	if err == nil {
		err = client.CoreV1().ResourceQuotas(namespace).Delete(quotaName, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Deleted Resource quota: %+v", quotaName))
	}
}

// newTestResourceQuota returns a quota that enforces default constraints for testing
func newTestResourceQuota(name string, size string, scName string) *v1.ResourceQuota {
	hard := v1.ResourceList{}
	// test quota on discovered resource type
	hard[v1.ResourceName(scName+rqStorageType)] = resource.MustParse(size)
	return &v1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.ResourceQuotaSpec{Hard: hard},
	}
}

// checkEventsforError prints the list of all events that occurred in the namespace and
// searches for expectedErrorMsg among these events
func checkEventsforError(client clientset.Interface, namespace string, listOptions metav1.ListOptions, expectedErrorMsg string) bool {
	eventList, _ := client.CoreV1().Events(namespace).List(listOptions)
	isFailureFound := false
	for _, item := range eventList.Items {
		ginkgo.By(fmt.Sprintf("EventList item: %q \n", item.Message))
		if strings.Contains(item.Message, expectedErrorMsg) {
			isFailureFound = true
			break
		}
	}
	return isFailureFound
}

// getNamespaceToRunTests returns the namespace in which the tests are expected to run
// For Vanilla & GuestCluster test setups: Returns random namespace name generated by the framework
// For SupervisorCluster test setup: Returns the user created namespace where pod vms will be provisioned
func getNamespaceToRunTests(f *framework.Framework) string {
	if supervisorCluster {
		return GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	}
	return f.Namespace.Name
}

func getVolumeIDFromSupervisorCluster(pvcName string) string {
	var svcClient clientset.Interface
	var err error
	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	svcPV := getPvFromClaim(svcClient, svNamespace, pvcName)
	volumeHandle := svcPV.Spec.CSI.VolumeHandle
	ginkgo.By(fmt.Sprintf("Found volume in Supervisor cluster with VolumeID: %s", volumeHandle))
	return volumeHandle
}

func verifyFilesExistOnVSphereVolume(namespace string, podName string, filePaths ...string) {
	for _, filePath := range filePaths {
		_, err := framework.RunKubectl("exec", fmt.Sprintf("--namespace=%s", namespace), podName, "--", "/bin/ls", filePath)
		framework.ExpectNoError(err, fmt.Sprintf("failed to verify file: %q on the pod: %q", filePath, podName))
	}
}

func createEmptyFilesOnVSphereVolume(namespace string, podName string, filePaths []string) {
	for _, filePath := range filePaths {
		err := framework.CreateEmptyFileOnPod(namespace, podName, filePath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

// CreateService creates a k8s service as described in the service.yaml present in the manifest path and returns that
// service to the caller
func CreateService(ns string, c clientset.Interface) *v1.Service {
	svcManifestFilePath := filepath.Join(manifestPath, "service.yaml")
	framework.Logf("Parsing service from %v", svcManifestFilePath)
	svc, err := manifest.SvcFromManifest(svcManifestFilePath)
	framework.ExpectNoError(err)

	_, err = c.CoreV1().Services(ns).Create(svc)
	framework.ExpectNoError(err)
	return svc
}

// GetStatefulSetFromManifest creates a StatefulSet from the statefulset.yaml file present in the manifest path
func GetStatefulSetFromManifest(ns string) *appsv1.StatefulSet {
	ssManifestFilePath := filepath.Join(manifestPath, "statefulset.yaml")
	framework.Logf("Parsing statefulset from %v", ssManifestFilePath)
	ss, err := manifest.StatefulSetFromManifest(ssManifestFilePath, ns)
	framework.ExpectNoError(err)
	return ss
}

// isDatastoreBelongsToDatacenterSpecifiedInConfig checks whether the given datastoreURL belongs to the datacenter specified in the vSphere.conf file
func isDatastoreBelongsToDatacenterSpecifiedInConfig(datastoreURL string) bool {
	var datacenters []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	dcList := strings.Split(cfg.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}
	for _, dc := range datacenters {
		defaultDatacenter, _ := finder.Datacenter(ctx, dc)
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err := getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		if defaultDatastore != nil && err == nil {
			return true
		}
	}

	// loop through all datacenters specified in conf file, and cannot find this given datastore
	return false
}

func getTargetvSANFileShareDatastoreURLsFromConfig() []string {
	var targetDsURLs []string
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if cfg.Global.TargetvSANFileShareDatastoreURLs != "" {
		targetDsURLs = strings.Split(cfg.Global.TargetvSANFileShareDatastoreURLs, ",")
	}
	return targetDsURLs
}

func isDatastorePresentinTargetvSANFileShareDatastoreURLs(datastoreURL string) bool {
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	datastoreURL = strings.TrimSpace(datastoreURL)
	targetDatastoreUrls := strings.Split(cfg.Global.TargetvSANFileShareDatastoreURLs, ",")
	for _, dsURL := range targetDatastoreUrls {
		dsURL = strings.TrimSpace(dsURL)
		if datastoreURL == dsURL {
			return true
		}
	}
	return false
}
func verifyVolumeExistInSupervisorCluster(pvcName string) bool {
	var svcClient clientset.Interface
	var err error
	if k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG"); k8senv != "" {
		svcClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	svNamespace := GetAndExpectStringEnvVar(envSupervisorClusterNamespace)
	_, err = svcClient.CoreV1().PersistentVolumeClaims(svNamespace).Get(pvcName, metav1.GetOptions{})
	return err == nil
}

// verifyCRDInSupervisor is a helper method to check if a given crd is created/deleted in the supervisor cluster
// This method will fetch the List of CRD Objects for a given crdName, Version and Group and then
// verifies if the given expectedInstanceName exist in the list
func verifyCRDInSupervisor(ctx context.Context, f *framework.Framework, expectedInstanceName string, crdName string, crdVersion string, crdGroup string, isCreated bool) {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dynamicClient, err := dynamic.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	resourceClient := dynamicClient.Resource(gvr).Namespace("")
	list, err := resourceClient.List(metav1.ListOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var instanceFound bool
	for _, crd := range list.Items {
		if crdName == "cnsnodevmattachments" {
			instance := &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSNodeVMAttachment crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}

		}
		if crdName == "cnsvolumemetadatas" {
			instance := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, instance)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if expectedInstanceName == instance.Name {
				ginkgo.By(fmt.Sprintf("Found CNSVolumeMetadata crd: %v, expected: %v", instance, expectedInstanceName))
				instanceFound = true
				break
			}
		}
	}
	if isCreated {
		gomega.Expect(instanceFound).To(gomega.BeTrue())
	} else {
		gomega.Expect(instanceFound).To(gomega.BeFalse())
	}
}

// trimQuotes takes a quoted string as input and returns the same string unquoted
func trimQuotes(str string) string {
	str = strings.TrimPrefix(str, "\"")
	str = strings.TrimSuffix(str, "\"")
	return str
}

/*
	readConfigFromSecretString takes input string of the form:
		[Global]
		insecure-flag = "true"
		cluster-id = "domain-c1047"
		[VirtualCenter "wdc-rdops-vm09-dhcp-238-224.eng.vmware.com"]
		user = "workload_storage_management-792c9cce-3cd2-4618-8853-52f521400e05@vsphere.local"
		password = "qd?\\/\"K=O_<ZQw~s4g(S"
		datacenters = "datacenter-1033"
		port = "443"
	Returns a de-serialized structured config data
*/
func readConfigFromSecretString(cfg string) (e2eTestConfig, error) {
	var config e2eTestConfig
	key, value := "", ""
	lines := strings.Split(cfg, "\n")
	for index, line := range lines {
		if index == 0 {
			// Skip [Global]
			continue
		}
		words := strings.Split(line, " = ")
		if len(words) == 1 {
			// case VirtualCenter
			words = strings.Split(line, " ")
			if strings.Contains(words[0], "VirtualCenter") {
				value = words[1]
				// Remove trailing '"]' characters from value
				value = strings.TrimSuffix(value, "]")
				config.Global.VCenterHostname = trimQuotes(value)
				fmt.Printf("Key: VirtualCenter, Value: %s\n", value)
			}
			continue
		}
		key = words[0]
		value = trimQuotes(words[1])
		var strconvErr error
		switch key {
		case "insecure-flag":
			if strings.Contains(value, "true") {
				config.Global.InsecureFlag = true
			} else {
				config.Global.InsecureFlag = false
			}
		case "cluster-id":
			config.Global.ClusterID = value
		case "user":
			config.Global.User = value
		case "password":
			config.Global.Password = value
		case "datacenters":
			config.Global.Datacenters = value
		case "port":
			config.Global.VCenterPort = value
		case "cnsregistervolumes-cleanup-intervalinmin":
			config.Global.CnsRegisterVolumesCleanupIntervalInMin, strconvErr = strconv.Atoi(value)
			gomega.Expect(strconvErr).NotTo(gomega.HaveOccurred())
		default:
			return config, fmt.Errorf("unknown key %s in the input string", key)
		}
	}
	return config, nil
}

// writeConfigToSecretString takes in a structured config data and serializes that into a string
func writeConfigToSecretString(cfg e2eTestConfig) (string, error) {
	result := fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"\n",
		cfg.Global.InsecureFlag, cfg.Global.VCenterHostname, cfg.Global.User, cfg.Global.Password, cfg.Global.Datacenters, cfg.Global.VCenterPort)
	return result, nil
}
