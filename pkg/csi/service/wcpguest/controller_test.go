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

package wcpguest

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snap "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	fakesnapshotclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const (
	testVolumeName = "pvc-12345"
	// The format of SupervisorPVCName is TanzuKubernetesClusterUID+"-"+ volumeUID.
	// The TanzuKubernetesClusterUID is empty in the unit test.
	testSupervisorPVCName = "-12345"
	testNamespace         = "test-namespace"
	testStorageClass      = "test-storageclass"
)

var (
	ctx                    context.Context
	isUnitTest             bool
	supervisorNamespace    string
	controllerTestInstance *controllerTest
	onceForControllerTest  sync.Once
)

type controllerTest struct {
	controller *controller
}

func configFromSim() (clientset.Interface, error) {
	isUnitTest = true
	supervisorClient := testclient.NewClientset()
	supervisorNamespace = testNamespace
	return supervisorClient, nil
}

func configFromEnvOrSim(ctx context.Context) (clientset.Interface, error) {
	cfg := &config.Config{}
	if err := config.FromEnvToGC(ctx, cfg); err != nil {
		return configFromSim()
	}
	isUnitTest = false
	restClientConfig := k8s.GetRestClientConfigForSupervisor(ctx, cfg.GC.Endpoint, cfg.GC.Port)
	supervisorClient, err := k8s.NewSupervisorClient(ctx, restClientConfig)
	if err != nil {
		return nil, err
	}
	return supervisorClient, nil
}

func getControllerTest(t *testing.T) *controllerTest {
	onceForControllerTest.Do(func() {
		// Create context.
		ctx = context.Background()
		supervisorClient, err := configFromEnvOrSim(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// Create fake snapshot client
		fakeSnapshotClient := fakesnapshotclient.NewSimpleClientset()

		c := &controller{
			supervisorClient:            supervisorClient,
			supervisorSnapshotterClient: fakeSnapshotClient,
			supervisorNamespace:         supervisorNamespace,
		}
		commonco.ContainerOrchestratorUtility, err =
			unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		if err != nil {
			t.Fatalf("Failed to create co agnostic interface. err=%v", err)
		}

		controllerTestInstance = &controllerTest{
			controller: c,
		}
	})
	return controllerTestInstance
}

func createVolume(ctx context.Context, ct *controllerTest, reqCreate *csi.CreateVolumeRequest,
	response chan *csi.CreateVolumeResponse, error chan error) {
	defer close(response)
	defer close(error)
	res, err := ct.controller.CreateVolume(ctx, reqCreate)
	response <- res
	error <- err
}

// TestGuestCreateVolume creates volume.
func TestGuestClusterControllerFlow(t *testing.T) {
	ct := getControllerTest(t)
	modes := []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	}
	for _, mode := range modes {

		// Create.
		params := make(map[string]string)

		params[common.AttributeSupervisorStorageClass] = testStorageClass
		if v := os.Getenv("SUPERVISOR_STORAGE_CLASS"); v != "" {
			params[common.AttributeSupervisorStorageClass] = v
		}
		capabilities := []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: mode,
				},
			},
		}

		reqCreate := &csi.CreateVolumeRequest{
			Name: testVolumeName,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1 * common.GbInBytes,
			},
			Parameters:         params,
			VolumeCapabilities: capabilities,
		}

		var respCreate *csi.CreateVolumeResponse
		var err error

		if isUnitTest {
			// Invoking CreateVolume in a separate thread and then setting the
			// Status to Bound explicitly.
			response := make(chan *csi.CreateVolumeResponse)
			error := make(chan error)

			go createVolume(ctx, ct, reqCreate, response, error)
			time.Sleep(1 * time.Second)
			pvc, _ := ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
				ct.controller.supervisorNamespace).Get(ctx, testSupervisorPVCName, metav1.GetOptions{})
			pvc.Status.Phase = "Bound"
			_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
				ct.controller.supervisorNamespace).Update(ctx, pvc, metav1.UpdateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			respCreate, err = <-response, <-error
		} else {
			respCreate, err = ct.controller.CreateVolume(ctx, reqCreate)
			// Wait for create volume finish.
			time.Sleep(1 * time.Second)
		}

		if err != nil {
			t.Fatal(err)
		}

		supervisorPVCName := respCreate.Volume.VolumeId
		// Verify the pvc has been created.
		_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
			ct.controller.supervisorNamespace).Get(ctx, supervisorPVCName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// Delete.
		reqDelete := &csi.DeleteVolumeRequest{
			VolumeId: supervisorPVCName,
		}
		_, err = ct.controller.DeleteVolume(ctx, reqDelete)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for delete volume finish.
		time.Sleep(1 * time.Second)
		// Verify the pvc has been deleted.
		_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
			ct.controller.supervisorNamespace).Get(ctx, supervisorPVCName, metav1.GetOptions{})
		if !errors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
}

// TestGuestClusterControllerFlowForTkgsHA creates volume.
func TestGuestClusterControllerFlowForTkgsHA(t *testing.T) {
	ct := getControllerTest(t)
	// Create.
	params := make(map[string]string)

	params[common.AttributeSupervisorStorageClass] = testStorageClass
	if v := os.Getenv("SUPERVISOR_STORAGE_CLASS"); v != "" {
		params[common.AttributeSupervisorStorageClass] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	topologyRequirement := createTestTopologyRequirement()

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:                params,
		VolumeCapabilities:        capabilities,
		AccessibilityRequirements: topologyRequirement,
	}

	var respCreate *csi.CreateVolumeResponse
	var err error

	if isUnitTest {
		// Invoking CreateVolume in a separate thread and then setting the
		// Status to Bound explicitly.
		response := make(chan *csi.CreateVolumeResponse)
		error := make(chan error)

		go createVolume(ctx, ct, reqCreate, response, error)
		time.Sleep(1 * time.Second)
		pvc, _ := ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
			ct.controller.supervisorNamespace).Get(ctx, testSupervisorPVCName, metav1.GetOptions{})
		// Update annotation on the supervisor PVC
		pvcAnnotations := make(map[string]string)
		pvcAnnotations[common.AnnVolumeAccessibleTopology] = `[{"R1" : "Zone1"}]`
		pvc.Annotations = pvcAnnotations
		pvc.Status.Phase = "Bound"
		_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
			ct.controller.supervisorNamespace).Update(ctx, pvc, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		respCreate, err = <-response, <-error
	} else {
		// TODO: Skip currently until supervisor side changes are completed
		t.Skipf("Skipping test until supervisor side changes are complete.")
		//respCreate, err = ct.controller.CreateVolume(ctx, reqCreate)
		// Wait for create volume finish.
		//time.Sleep(1 * time.Second)
		return
	}

	if err != nil {
		t.Fatal(err)
	}

	// Verify the response to ensure Accessibility topology is set.
	if respCreate.Volume.AccessibleTopology == nil {
		t.Fatalf("AccessibleTopology was unset when volume was created with topology on guest cluster")
	}

	// Retrieve the segments
	respAccessibleTopology := respCreate.Volume.AccessibleTopology[0].Segments
	if val, ok := respAccessibleTopology["R1"]; !ok {
		t.Fatalf("AccessibleTopology inccorectly populated, key not present")
	} else {
		if val != "Zone1" {
			t.Fatalf("AccessibleTopology inccorectly populated, value incorrect")
		}
	}
	t.Log("AccessibleTopology was correctly set in create volume response")

	supervisorPVCName := respCreate.Volume.VolumeId
	// Verify the pvc has been created.
	_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
		ct.controller.supervisorNamespace).Get(ctx, supervisorPVCName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Delete.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: supervisorPVCName,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for delete volume finish.
	time.Sleep(1 * time.Second)
	// Verify the pvc has been deleted.
	_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
		ct.controller.supervisorNamespace).Get(ctx, supervisorPVCName, metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Fatal(err)
	}
}

// TestGuestClusterControllerFlowForWorkloadDomainIsolation creates file volume with topology.
func TestGuestClusterControllerFlowForWorkloadDomainIsolation(t *testing.T) {
	ct := getControllerTest(t)
	// Create.
	params := make(map[string]string)

	params[common.AttributeSupervisorStorageClass] = testStorageClass
	if v := os.Getenv("SUPERVISOR_STORAGE_CLASS"); v != "" {
		params[common.AttributeSupervisorStorageClass] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}

	topologyRequirement := createTestTopologyRequirement()

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:                params,
		VolumeCapabilities:        capabilities,
		AccessibilityRequirements: topologyRequirement,
	}

	var respCreate *csi.CreateVolumeResponse
	var err error

	if isUnitTest {
		// Invoking CreateVolume in a separate thread and then setting the
		// Status to Bound explicitly.
		response := make(chan *csi.CreateVolumeResponse)
		error := make(chan error)

		go createVolume(ctx, ct, reqCreate, response, error)
		time.Sleep(1 * time.Second)
		pvc, _ := ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
			ct.controller.supervisorNamespace).Get(ctx, testSupervisorPVCName, metav1.GetOptions{})
		// Update annotation on the supervisor PVC
		pvcAnnotations := make(map[string]string)
		pvcAnnotations[common.AnnVolumeAccessibleTopology] = `[{"R1" : "Zone1"}]`
		pvc.Annotations = pvcAnnotations
		pvc.Status.Phase = "Bound"
		_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
			ct.controller.supervisorNamespace).Update(ctx, pvc, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		respCreate, err = <-response, <-error
	} else {
		// TODO: Skip currently until supervisor side changes are completed
		t.Skipf("Skipping test until supervisor side changes are complete.")
		//respCreate, err = ct.controller.CreateVolume(ctx, reqCreate)
		// Wait for create volume finish.
		//time.Sleep(1 * time.Second)
		return
	}

	if err != nil {
		t.Fatal(err)
	}

	// Verify the response to ensure Accessibility topology is set.
	if respCreate.Volume.AccessibleTopology == nil {
		t.Fatalf("AccessibleTopology was unset when volume was created with topology on guest cluster")
	}

	// Retrieve the segments
	respAccessibleTopology := respCreate.Volume.AccessibleTopology[0].Segments
	if val, ok := respAccessibleTopology["R1"]; !ok {
		t.Fatalf("AccessibleTopology inccorectly populated, key not present")
	} else {
		if val != "Zone1" {
			t.Fatalf("AccessibleTopology inccorectly populated, value incorrect")
		}
	}
	t.Log("AccessibleTopology was correctly set in create volume response")

	supervisorPVCName := respCreate.Volume.VolumeId
	// Verify the pvc has been created.
	_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
		ct.controller.supervisorNamespace).Get(ctx, supervisorPVCName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Delete.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: supervisorPVCName,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for delete volume finish.
	time.Sleep(1 * time.Second)
	// Verify the pvc has been deleted.
	_, err = ct.controller.supervisorClient.CoreV1().PersistentVolumeClaims(
		ct.controller.supervisorNamespace).Get(ctx, supervisorPVCName, metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Fatal(err)
	}
}

func createTestTopologyRequirement() *csi.TopologyRequirement {
	// Create a dummy topology requirement.
	segment := make(map[string]string)
	segment["R1"] = "Zone1"
	topology := &csi.Topology{
		Segments: segment,
	}
	topologyRequirement := &csi.TopologyRequirement{
		Requisite: []*csi.Topology{topology},
		Preferred: []*csi.Topology{topology},
	}
	return topologyRequirement
}

// TestGenerateVolumeAccessibleTopologyFromPVCAnnotation helps unit test
// generateVolumeAccessibleTopologyFromPVCAnnotation function.
func TestGenerateVolumeAccessibleTopologyFromPVCAnnotation(t *testing.T) {
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "ns",
		},
	}
	claim.Annotations = make(map[string]string)
	claim.Annotations[common.AnnVolumeAccessibleTopology] =
		"[{\"topology.kubernetes.io/zone\":\"zone1\"},{\"topology.kubernetes.io/zone\":\"zone2\"}]"

	t.Logf("Calling generateVolumeAccessibleTopologyFromPVCAnnotation with "+
		"%s annotation value %q", common.AnnVolumeAccessibleTopology,
		claim.Annotations[common.AnnVolumeAccessibleTopology])

	accessibleTopologies, err := generateVolumeAccessibleTopologyFromPVCAnnotation(claim)
	if err != nil {
		t.Fatalf("failed to generate AccessibilityRequirements from PVC annotation. Err: %v", err)
	}

	expectedAccessibleTopologies := []map[string]string{
		{"topology.kubernetes.io/zone": "zone1"},
		{"topology.kubernetes.io/zone": "zone2"},
	}

	if !reflect.DeepEqual(accessibleTopologies, expectedAccessibleTopologies) {
		t.Fatalf("accessibleTopologies %v does not match with expectedAccessibleTopologies: %v",
			accessibleTopologies, expectedAccessibleTopologies)
	}
	t.Logf("accessibleTopologies %v match with expectedAccessibleTopologies: %v",
		accessibleTopologies, expectedAccessibleTopologies)
}

// TestGenerateVolumeAccessibleTopologyFromInvalidPVCAnnotation helps unit test
// generateVolumeAccessibleTopologyFromPVCAnnotation function for ill-formed annotation.
func TestGenerateVolumeAccessibleTopologyFromInvalidPVCAnnotation(t *testing.T) {
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "ns",
		},
	}
	claim.Annotations = make(map[string]string)
	claim.Annotations[common.AnnVolumeAccessibleTopology] =
		"[{\"topology.kubernetes.io/zone\":\"zone1\",{\"topology.kubernetes.io/zone\":\"zone2\"}]"

	t.Logf("Calling generateVolumeAccessibleTopologyFromPVCAnnotation with "+
		"%s annotation value %q", common.AnnVolumeAccessibleTopology,
		claim.Annotations[common.AnnVolumeAccessibleTopology])

	accessibleTopologies, err := generateVolumeAccessibleTopologyFromPVCAnnotation(claim)
	if err == nil {
		t.Fatalf("generateVolumeAccessibleTopologyFromPVCAnnotation should have failed. "+
			"accessibleTopologies: %v", accessibleTopologies)
	} else {
		t.Logf("expected error: %v", err)
	}
}

// TestGenerateGuestClusterRequestedTopologyJSON helps unit test
// generateGuestClusterRequestedTopologyJSON function
func TestGenerateGuestClusterRequestedTopologyJSON(t *testing.T) {
	volumeAccessibleTopology := make([]*csi.Topology, 0)
	volumeAccessibleTopology = append(volumeAccessibleTopology,
		&csi.Topology{Segments: map[string]string{"topology.kubernetes.io/zone": "zone1"}})
	volumeAccessibleTopology = append(volumeAccessibleTopology,
		&csi.Topology{Segments: map[string]string{"topology.kubernetes.io/zone": "zone2"}})

	t.Logf("Calling generateGuestClusterRequestedTopologyJSON with topologies: %v", volumeAccessibleTopology)
	volumeAccessibleTopologyJSON, err := generateGuestClusterRequestedTopologyJSON(volumeAccessibleTopology)
	if err != nil {
		t.Fatalf("failed to generate json string from volumeAccessibleTopology. Err: %v", err)
	}
	expectedVolumeAccessibleTopologyJSON :=
		"[{\"topology.kubernetes.io/zone\":\"zone1\"},{\"topology.kubernetes.io/zone\":\"zone2\"}]"

	if !reflect.DeepEqual(volumeAccessibleTopologyJSON, expectedVolumeAccessibleTopologyJSON) {
		t.Fatalf("volumeAccessibleTopologyJSON %v does not match with expectedVolumeAccessibleTopologyJSON: %v",
			volumeAccessibleTopologyJSON, expectedVolumeAccessibleTopologyJSON)
	}
	t.Logf("volumeAccessibleTopologyJSON %v match with expectedVolumeAccessibleTopologyJSON: %v",
		volumeAccessibleTopologyJSON, expectedVolumeAccessibleTopologyJSON)
}

func TestVirtualMachineVolumePatchWithOptimisticMerge(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = vmoperatortypes.AddToScheme(scheme)

	client := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-vm",
				Namespace: "my-namespace",
			},
			Spec: vmoperatortypes.VirtualMachineSpec{
				Volumes: []vmoperatortypes.VirtualMachineVolume{
					{
						Name: "my-vol-1",
						VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
							PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
								PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
									ClaimName: "my-pvc-1",
								},
							},
						},
					},
				},
			},
		}).
		WithStatusSubresource(&vmoperatortypes.VirtualMachine{}).
		Build()

	var (
		vm1 vmoperatortypes.VirtualMachine
		ctx = context.Background()
		key = ctrlclient.ObjectKey{Name: "my-vm", Namespace: "my-namespace"}
	)

	if err := client.Get(ctx, key, &vm1); err != nil {
		t.Fatal(err)
	}

	addVolumePatch := ctrlclient.MergeFromWithOptions(
		vm1.DeepCopy(),
		ctrlclient.MergeFromWithOptimisticLock{})

	vm1.Spec.Volumes = append(
		vm1.Spec.Volumes,
		vmoperatortypes.VirtualMachineVolume{
			Name: "my-vol-2",
			VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
				PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
						ClaimName: "my-pvc-2",
					},
				},
			},
		})

	if err := client.Patch(ctx, &vm1, addVolumePatch); err != nil {
		t.Fatal(err)
	}

	var vm2 vmoperatortypes.VirtualMachine
	if err := client.Get(ctx, key, &vm2); err != nil {
		t.Fatal(err)
	}

	if a, e := len(vm2.Spec.Volumes), 2; a != e {
		t.Fatalf("invalid number of volumes: a=%d, e=%d", a, e)
	}
	if a, e := vm2.Spec.Volumes[0].Name, "my-vol-1"; a != e {
		t.Fatalf("invalid volume name: a=%s, e=%s", a, e)
	}
	if a, e := vm2.Spec.Volumes[1].Name, "my-vol-2"; a != e {
		t.Fatalf("invalid volume name: a=%s, e=%s", a, e)
	}

	rmVolumePatch := ctrlclient.MergeFromWithOptions(
		vm2.DeepCopy(),
		ctrlclient.MergeFromWithOptimisticLock{})

	vm2.Spec.Volumes = []vmoperatortypes.VirtualMachineVolume{
		{
			Name: "my-vol-2",
			VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
				PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
						ClaimName: "my-pvc-2",
					},
				},
			},
		},
	}

	if err := client.Patch(ctx, &vm2, rmVolumePatch); err != nil {
		t.Fatal(err)
	}

	var vm3 vmoperatortypes.VirtualMachine
	if err := client.Get(ctx, key, &vm3); err != nil {
		t.Fatal(err)
	}

	if a, e := len(vm3.Spec.Volumes), 1; a != e {
		t.Fatalf("invalid number of volumes: a=%d, e=%d", a, e)
	}
	if a, e := vm3.Spec.Volumes[0].Name, "my-vol-2"; a != e {
		t.Fatalf("invalid volume name: a=%s, e=%s", a, e)
	}
}

// TestPatchObjectFunctionality tests the PatchObject functionality used in wcpguest controller
func TestPatchObjectFunctionality(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	err := v1.AddToScheme(scheme)
	require.NoError(t, err)

	tests := []struct {
		name        string
		setupPVC    func() *v1.PersistentVolumeClaim
		modifyPVC   func(*v1.PersistentVolumeClaim)
		expectError bool
	}{
		{
			name: "Successfully patch PVC finalizer removal",
			setupPVC: func() *v1.PersistentVolumeClaim {
				return &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "test-ns",
						Finalizers: []string{
							cnsoperatortypes.CNSVolumeFinalizer,
							"other-finalizer",
						},
					},
					Spec: v1.PersistentVolumeClaimSpec{
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				}
			},
			modifyPVC: func(pvc *v1.PersistentVolumeClaim) {
				// Remove CNS finalizer (simulating finalizer removal logic)
				for i, finalizer := range pvc.ObjectMeta.Finalizers {
					if finalizer == cnsoperatortypes.CNSVolumeFinalizer {
						pvc.ObjectMeta.Finalizers = append(pvc.ObjectMeta.Finalizers[:i], pvc.ObjectMeta.Finalizers[i+1:]...)
						break
					}
				}
			},
			expectError: false,
		},
		{
			name: "Successfully patch PVC storage size increase",
			setupPVC: func() *v1.PersistentVolumeClaim {
				return &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "test-ns",
					},
					Spec: v1.PersistentVolumeClaimSpec{
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				}
			},
			modifyPVC: func(pvc *v1.PersistentVolumeClaim) {
				// Increase storage size (simulating expansion logic)
				pvc.Spec.Resources.Requests[v1.ResourceStorage] = resource.MustParse("2Gi")
			},
			expectError: false,
		},
		{
			name: "Handle PVC with no changes gracefully",
			setupPVC: func() *v1.PersistentVolumeClaim {
				return &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "test-ns",
					},
					Spec: v1.PersistentVolumeClaimSpec{
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				}
			},
			modifyPVC: func(pvc *v1.PersistentVolumeClaim) {
				// No changes - should still work
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			pvc := tt.setupPVC()
			fakeClient := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pvc).
				Build()

			// Create a copy for the original
			original := pvc.DeepCopy()

			// Apply modifications
			tt.modifyPVC(pvc)

			// Test PatchObject
			err := k8s.PatchObject(ctx, fakeClient, original, pvc)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify the patch was applied
				updatedPVC := &v1.PersistentVolumeClaim{}
				err = fakeClient.Get(ctx, ctrlclient.ObjectKey{
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
				}, updatedPVC)
				assert.NoError(t, err)

				// Verify specific changes based on test case
				if tt.name == "Successfully patch PVC finalizer removal" {
					assert.NotContains(t, updatedPVC.Finalizers, cnsoperatortypes.CNSVolumeFinalizer)
					assert.Contains(t, updatedPVC.Finalizers, "other-finalizer")
				} else if tt.name == "Successfully patch PVC storage size increase" {
					expectedSize := resource.MustParse("2Gi")
					actualSize := updatedPVC.Spec.Resources.Requests[v1.ResourceStorage]
					assert.True(t, expectedSize.Equal(actualSize))
				}
			}
		})
	}
}

// TestControllerRuntimeClientCreation tests the controller-runtime client creation logic
func TestControllerRuntimeClientCreation(t *testing.T) {
	t.Run("Client creation with valid config", func(t *testing.T) {
		// Create a minimal rest config for testing
		config := &rest.Config{
			Host: "https://test-cluster",
		}

		// This should not fail even with a test config
		client, err := ctrlclient.New(config, ctrlclient.Options{})

		// We expect this to work in the test environment
		// The actual connection will fail, but client creation should succeed
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("Client creation with nil config should fail", func(t *testing.T) {
		client, err := ctrlclient.New(nil, ctrlclient.Options{})

		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

// TestPatchObjectErrorHandling tests error scenarios in PatchObject usage
func TestPatchObjectErrorHandling(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	err := v1.AddToScheme(scheme)
	require.NoError(t, err)

	t.Run("PatchObject with non-existent object", func(t *testing.T) {
		fakeClient := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "non-existent-pvc",
				Namespace: "test-ns",
			},
		}
		original := pvc.DeepCopy()
		pvc.Labels = map[string]string{"test": "label"}

		err := k8s.PatchObject(ctx, fakeClient, original, pvc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("PatchObject with invalid patch data", func(t *testing.T) {
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-ns",
			},
		}
		fakeClient := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pvc).
			Build()

		// Create original and modified objects that would create an invalid patch
		original := pvc.DeepCopy()
		// This should still work as the fake client is permissive
		err := k8s.PatchObject(ctx, fakeClient, original, pvc)
		assert.NoError(t, err) // Fake client allows this
	})
}

// TestCreateVolumeAnnotationLogic tests the annotation creation logic in isolation
func TestCreateVolumeAnnotationLogic(t *testing.T) {
	ctx := context.Background()

	// Setup global container orchestrator utility (required for feature switch)
	originalCO := commonco.ContainerOrchestratorUtility

	// Create properly initialized fake container orchestrator
	fakeOrchestrator, coErr := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, coErr)

	// Enable the ImprovedVolumeVisibility feature switch
	err := fakeOrchestrator.EnableFSS(ctx, common.SupervisorImproveVisiblity)
	require.NoError(t, err)

	commonco.ContainerOrchestratorUtility = fakeOrchestrator
	defer func() {
		commonco.ContainerOrchestratorUtility = originalCO
	}()

	// Setup test controller
	supervisorClient := testclient.NewClientset()

	controller := &controller{
		supervisorClient:           supervisorClient,
		supervisorNamespace:        "test-namespace",
		tanzukubernetesClusterUID:  "test-cluster-uid",
		tanzukubernetesClusterName: "test-cluster-name",
		guestClusterDist:           "test-distribution",
	}

	t.Run("Test annotation and label creation for PVC", func(t *testing.T) {
		// Test the annotation creation logic by directly creating a PVC with annotations
		// This simulates what CreateVolume does internally when external-provisioner
		// sets the PVC metadata parameters with --extra-create-metadata flag

		pvcName := "test-pvc-name"
		pvcNamespace := "test-pvc-namespace"

		// Create the annotations map as done in CreateVolume
		labels := make(map[string]string)
		annotations := make(map[string]string)

		// Add guest cluster label (from CreateVolume logic)
		key := controller.tanzukubernetesClusterName + "/" + controller.guestClusterDist
		labels[key] = controller.tanzukubernetesClusterUID

		// Create guest cluster annotation (from CreateVolume logic)
		guestClusterAnnot := make(map[string]string)
		guestClusterAnnot["clusterId"] = controller.tanzukubernetesClusterUID
		guestClusterAnnot["clusterName"] = controller.tanzukubernetesClusterName
		guestClusterAnnot["clusterDist"] = controller.guestClusterDist
		guestClusterAnnot["pvcName"] = pvcName
		guestClusterAnnot["pvcNamespace"] = pvcNamespace

		guestClusterAnnotJSON, err := json.Marshal(guestClusterAnnot)
		require.NoError(t, err, "Should marshal guest cluster annotation")
		annotations[common.AnnKeyGuestClusterPvc] = string(guestClusterAnnotJSON)

		// Create PVC with annotations and labels
		supervisorPVC := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-supervisor-pvc",
				Namespace:   controller.supervisorNamespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
			},
		}

		_, err = supervisorClient.CoreV1().PersistentVolumeClaims(controller.supervisorNamespace).
			Create(ctx, supervisorPVC, metav1.CreateOptions{})
		require.NoError(t, err)

		// Verify the PVC was created with correct annotations and labels
		createdPVC, err := supervisorClient.CoreV1().PersistentVolumeClaims(controller.supervisorNamespace).
			Get(ctx, "test-supervisor-pvc", metav1.GetOptions{})

		require.NoError(t, err)

		// Verify guest cluster label
		expectedLabelKey := controller.tanzukubernetesClusterName + "/" + controller.guestClusterDist
		assert.Equal(t, controller.tanzukubernetesClusterUID, createdPVC.Labels[expectedLabelKey],
			"Guest cluster label should match")

		// Verify guest cluster annotation
		guestClusterAnnotationJSON := createdPVC.Annotations[common.AnnKeyGuestClusterPvc]
		assert.NotEmpty(t, guestClusterAnnotationJSON, "Guest cluster annotation should be present")

		var retrievedAnnotation map[string]string
		err = json.Unmarshal([]byte(guestClusterAnnotationJSON), &retrievedAnnotation)
		require.NoError(t, err, "Guest cluster annotation should be valid JSON")

		// Verify annotation content
		assert.Equal(t, controller.tanzukubernetesClusterUID, retrievedAnnotation["clusterId"])
		assert.Equal(t, controller.tanzukubernetesClusterName, retrievedAnnotation["clusterName"])
		assert.Equal(t, controller.guestClusterDist, retrievedAnnotation["clusterDist"])
		assert.Equal(t, pvcName, retrievedAnnotation["pvcName"])
		assert.Equal(t, pvcNamespace, retrievedAnnotation["pvcNamespace"])
	})

	t.Run("Test annotation creation without PVC parameters", func(t *testing.T) {
		// Test when PVC name/namespace are missing

		labels := make(map[string]string)
		annotations := make(map[string]string)

		// Add guest cluster label
		key := controller.tanzukubernetesClusterName + "/" + controller.guestClusterDist
		labels[key] = controller.tanzukubernetesClusterUID

		// Create guest cluster annotation without PVC info
		guestClusterAnnot := make(map[string]string)
		guestClusterAnnot["clusterId"] = controller.tanzukubernetesClusterUID
		guestClusterAnnot["clusterName"] = controller.tanzukubernetesClusterName
		guestClusterAnnot["clusterDist"] = controller.guestClusterDist
		// No pvcName and pvcNamespace

		guestClusterAnnotJSON, err := json.Marshal(guestClusterAnnot)
		require.NoError(t, err)
		annotations[common.AnnKeyGuestClusterPvc] = string(guestClusterAnnotJSON)

		// Create PVC
		supervisorPVC := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-supervisor-pvc-no-params",
				Namespace:   controller.supervisorNamespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
			},
		}

		_, err = supervisorClient.CoreV1().PersistentVolumeClaims(controller.supervisorNamespace).
			Create(ctx, supervisorPVC, metav1.CreateOptions{})
		require.NoError(t, err)

		// Verify annotation content
		createdPVC, err := supervisorClient.CoreV1().PersistentVolumeClaims(controller.supervisorNamespace).
			Get(ctx, "test-supervisor-pvc-no-params", metav1.GetOptions{})
		require.NoError(t, err)

		guestClusterAnnotationJSON := createdPVC.Annotations[common.AnnKeyGuestClusterPvc]
		var retrievedAnnotation map[string]string
		err = json.Unmarshal([]byte(guestClusterAnnotationJSON), &retrievedAnnotation)
		require.NoError(t, err)

		// Should have cluster info but not PVC info
		assert.Equal(t, controller.tanzukubernetesClusterUID, retrievedAnnotation["clusterId"])
		assert.Equal(t, controller.tanzukubernetesClusterName, retrievedAnnotation["clusterName"])
		assert.Equal(t, controller.guestClusterDist, retrievedAnnotation["clusterDist"])
		assert.Empty(t, retrievedAnnotation["pvcName"])
		assert.Empty(t, retrievedAnnotation["pvcNamespace"])
	})

}

// TestCreateVolumeUpdatesVolumeNameAnnotation verifies that CreateVolume patches the
// supervisor PVC with the bound PV name inside the AnnKeyGuestClusterPvc annotation
// when the SupervisorImproveVisiblity FSS is enabled.
//
// The key behavior under test is that isPVCInSupervisorClusterBound now returns the
// bound PVC object, so CreateVolume can use it directly (no extra GET round-trip) to
// write volumeName into the annotation via a MergePatch.
func TestCreateVolumeUpdatesVolumeNameAnnotation(t *testing.T) {
	tCtx := context.Background()

	// Fresh, isolated client and controller — do not share with the sync.Once singleton.
	supervisorClient := testclient.NewClientset()
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)
	require.NoError(t, co.EnableFSS(tCtx, common.SupervisorImproveVisiblity))

	oldCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = co
	defer func() { commonco.ContainerOrchestratorUtility = oldCO }()

	const (
		clusterUID = "test-uid"
		// CreateVolume strips the leading "pvc-" prefix before building the supervisor PVC
		// name: supervisorPVCName = tanzukubernetesClusterUID + "-" + req.Name[4:]
		volReqName = "pvc-vol-abc"
		svPVCName  = clusterUID + "-vol-abc"
		// PV name that the supervisor cluster assigns once the PVC is bound.
		svPVName = "pv-test-backing"
	)

	c := &controller{
		supervisorClient:           supervisorClient,
		supervisorNamespace:        testNamespace,
		tanzukubernetesClusterUID:  clusterUID,
		tanzukubernetesClusterName: "test-cluster",
		guestClusterDist:           "test-dist",
	}

	reqCreate := &csi.CreateVolumeRequest{
		Name: volReqName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters: map[string]string{
			common.AttributeSupervisorStorageClass: testStorageClass,
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}},
	}

	type cvResult struct {
		resp *csi.CreateVolumeResponse
		err  error
	}
	resultCh := make(chan cvResult, 1)
	go func() {
		resp, err := c.CreateVolume(tCtx, reqCreate)
		resultCh <- cvResult{resp, err}
	}()

	// Wait long enough for CreateVolume to create the supervisor PVC and start the Watch.
	time.Sleep(1 * time.Second)

	// Simulate the supervisor cluster binding the PVC and assigning a backing PV name.
	// Because the fake client's Update propagates through the Watch channel, this wakes
	// up isPVCInSupervisorClusterBound and returns the object as boundPVC.
	pvc, err := supervisorClient.CoreV1().PersistentVolumeClaims(testNamespace).
		Get(tCtx, svPVCName, metav1.GetOptions{})
	require.NoError(t, err, "supervisor PVC should exist once CreateVolume has started")

	pvc.Status.Phase = v1.ClaimBound
	pvc.Spec.VolumeName = svPVName
	_, err = supervisorClient.CoreV1().PersistentVolumeClaims(testNamespace).
		Update(tCtx, pvc, metav1.UpdateOptions{})
	require.NoError(t, err)

	r := <-resultCh
	require.NoError(t, r.err, "CreateVolume should succeed once PVC is bound")

	// After CreateVolume returns, it must have already MergePatch-ed the supervisor PVC
	// with volumeName — no extra GET was needed because isPVCInSupervisorClusterBound
	// now returns the fresh bound object directly.
	updated, err := supervisorClient.CoreV1().PersistentVolumeClaims(testNamespace).
		Get(tCtx, svPVCName, metav1.GetOptions{})
	require.NoError(t, err)

	annotJSON, ok := updated.Annotations[common.AnnKeyGuestClusterPvc]
	require.True(t, ok, "AnnKeyGuestClusterPvc annotation must be present after bind")

	var annot map[string]string
	require.NoError(t, json.Unmarshal([]byte(annotJSON), &annot))

	// Core assertion: the PV name assigned by the supervisor must appear in the annotation.
	assert.Equal(t, svPVName, annot["volumeName"],
		"volumeName in annotation must equal the PV name returned by the supervisor cluster")

	// Fields written at creation time must survive the MergePatch.
	assert.Equal(t, clusterUID, annot["clusterId"])
	assert.Equal(t, "test-cluster", annot["clusterName"])
}

// annotateSnapshotSpy embeds FakeK8SOrchestrator and records the exact arguments
// passed to AnnotateVolumeSnapshot so tests can assert on them without relying
// on side-effects in an external client.
type annotateSnapshotSpy struct {
	*unittestcommon.FakeK8SOrchestrator
	calledName        string
	calledNamespace   string
	calledAnnotations map[string]string
}

func (s *annotateSnapshotSpy) AnnotateVolumeSnapshot(_ context.Context,
	volumeSnapshotName, volumeSnapshotNamespace string,
	annotations map[string]string) (bool, error) {
	s.calledName = volumeSnapshotName
	s.calledNamespace = volumeSnapshotNamespace
	s.calledAnnotations = annotations
	return true, nil
}

// TestCreateSnapshotWithAnnotations verifies the full CreateSnapshot path in the
// guest controller: supervisor PVC lookup, VolumeSnapshot readiness polling, and
// guest-cluster annotation propagation.
//
// The supervisor VolumeSnapshot is pre-created in the fake snapshotter client with
// ReadyToUse=true so that IsVolumeSnapshotReady returns on the first (immediate)
// poll without any real-time wait.
func TestCreateSnapshotWithAnnotations(t *testing.T) {
	ctx := context.Background()

	fakeOrch, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)
	spy := &annotateSnapshotSpy{
		FakeK8SOrchestrator: fakeOrch.(*unittestcommon.FakeK8SOrchestrator),
	}
	oldCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = spy
	defer func() { commonco.ContainerOrchestratorUtility = oldCO }()
	require.NoError(t, spy.EnableFSS(ctx, common.SupervisorImproveVisiblity))

	// Use isolated fake clients so this test does not interfere with the shared
	// singleton controller used by the other tests in this package.
	const (
		snapReqName        = "snapshot-mysnap"
		sourcePVCName      = "source-pvc"
		guestSnapName      = "snapshot-mysnap"
		guestSnapNamespace = "guest-ns"
		// CreateSnapshot derives the supervisor snapshot name as:
		//   tanzukubernetesClusterUID + "-" + req.Name[9:]
		// The code assumes a "snapshot-" prefix (9 chars). With clusterUID
		// "tkc-uid" and req.Name = "snapshot-mysnap", supervisorSnapName = "tkc-uid-mysnap".
		clusterUID         = "tkc-uid"
		clusterName        = "my-tkc"
		supervisorSnapName = "tkc-uid-mysnap"
		wantSnapshotInfo   = "fcd-abc+snap-xyz"
		wantChangeID       = "change-id-123"
		wantVSCName        = "snapcontent-tkc-uid-mysnap"
	)

	supervisorFakeClient := testclient.NewClientset()
	fakeSnapshotClient := fakesnapshotclient.NewSimpleClientset()
	c := &controller{
		supervisorClient:            supervisorFakeClient,
		supervisorSnapshotterClient: fakeSnapshotClient,
		supervisorNamespace:         testNamespace,
		tanzukubernetesClusterUID:   clusterUID,
		tanzukubernetesClusterName:  clusterName,
	}

	// Create the supervisor PVC that backs the snapshot request.
	_, err = supervisorFakeClient.CoreV1().PersistentVolumeClaims(testNamespace).Create(ctx,
		&v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sourcePVCName,
				Namespace: testNamespace,
			},
		}, metav1.CreateOptions{})
	require.NoError(t, err)

	// Register a reactor that enriches every supervisor VolumeSnapshot created by
	// the controller with ReadyToUse=true status and the info/change-id annotations
	// that the SV snapshot controller would normally populate. This lets
	// IsVolumeSnapshotReady return on the very first poll (immediate=true), keeping
	// the test fast while exercising the full annotation path.
	// Returning (false, nil, nil) lets the default object-tracker also handle the
	// Create, so the enriched VS is stored and subsequent Get calls find it.
	readyToUse := true
	creationTime := metav1.Now()
	restoreSize := resource.MustParse("1Gi")
	fakeSnapshotClient.PrependReactor("create", "volumesnapshots",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			vs := action.(ktesting.CreateAction).GetObject().(*snap.VolumeSnapshot)
			vs.Status = &snap.VolumeSnapshotStatus{
				ReadyToUse:   &readyToUse,
				CreationTime: &creationTime,
				RestoreSize:  &restoreSize,
			}
			if vs.Annotations == nil {
				vs.Annotations = make(map[string]string)
			}
			vs.Annotations[common.VolumeSnapshotInfoKey] = wantSnapshotInfo
			vs.Annotations[common.VolumeSnapshotChangeIDKey] = wantChangeID
			return false, nil, nil
		})

	t.Run("returns correct snapshot fields on success", func(t *testing.T) {
		resp, err := c.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
			Name:           snapReqName,
			SourceVolumeId: sourcePVCName,
			Parameters: map[string]string{
				common.AttributeSupervisorVolumeSnapshotClass: "test-snapshot-class",
				common.VolumeSnapshotNameKey:                  guestSnapName,
				common.VolumeSnapshotNamespaceKey:             guestSnapNamespace,
				common.VolumeSnapshotContentNameKey:           wantVSCName,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Snapshot)
		assert.Equal(t, supervisorSnapName, resp.Snapshot.SnapshotId)
		assert.Equal(t, sourcePVCName, resp.Snapshot.SourceVolumeId)
		assert.True(t, resp.Snapshot.ReadyToUse)
		assert.NotNil(t, resp.Snapshot.CreationTime)

		// Verify AnnotateVolumeSnapshot was called with the correct target.
		assert.Equal(t, guestSnapName, spy.calledName)
		assert.Equal(t, guestSnapNamespace, spy.calledNamespace)

		// Verify snapshot-info and change-id annotations are mirrored from the
		// supervisor VolumeSnapshot to the guest VolumeSnapshot.
		assert.Equal(t, wantSnapshotInfo, spy.calledAnnotations[common.VolumeSnapshotInfoKey])
		assert.Equal(t, wantChangeID, spy.calledAnnotations[common.VolumeSnapshotChangeIDKey])

		// Verify that AnnKeyGuestClusterSnapshot is NOT on the guest VS annotations —
		// it is patched directly on the supervisor VolumeSnapshot instead.
		assert.Empty(t, spy.calledAnnotations[common.AnnKeyGuestClusterSnapshot],
			"guest-cluster-snapshot annotation must NOT be sent to the guest VS")

		// Verify the supervisor VolumeSnapshot was patched with AnnKeyGuestClusterSnapshot
		// carrying all expected fields: name, namespace, clusterName, sourceVolumeId,
		// and volumeSnapshotContentName.
		updatedSVS, err := fakeSnapshotClient.SnapshotV1().VolumeSnapshots(testNamespace).
			Get(ctx, supervisorSnapName, metav1.GetOptions{})
		require.NoError(t, err)
		var svAnnot map[string]string
		require.NoError(t, json.Unmarshal(
			[]byte(updatedSVS.Annotations[common.AnnKeyGuestClusterSnapshot]), &svAnnot))
		assert.Equal(t, snapReqName, svAnnot["name"])
		assert.Equal(t, guestSnapNamespace, svAnnot["namespace"])
		assert.Equal(t, clusterName, svAnnot["clusterName"])
		assert.Equal(t, sourcePVCName, svAnnot["sourceVolumeId"])
		assert.Equal(t, wantVSCName, svAnnot["volumeSnapshotContentName"])
	})
}
