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
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
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
	supervisorClient := testclient.NewSimpleClientset()
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

		c := &controller{
			supervisorClient:    supervisorClient,
			supervisorNamespace: supervisorNamespace,
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
