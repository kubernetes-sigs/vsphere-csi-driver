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
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
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
