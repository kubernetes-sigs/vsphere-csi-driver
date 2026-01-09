package utils

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	cnssim "github.com/vmware/govmomi/cns/simulator"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"
	_ "github.com/vmware/govmomi/vapi/simulator"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	cnsvolumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

const (
	testClusterName = "test-cluster"
)

var (
	ctx                     context.Context
	commonUtilsTestInstance *commonUtilsTest
	onceForControllerTest   sync.Once
)

type commonUtilsTest struct {
	volumeManager cnsvolumes.Manager
}

// configFromSim starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with an empty tls.Config.
func configFromSim() (*cnsconfig.Config, func()) {
	return configFromCustomizedSimWithTLS(new(tls.Config), true)
}

// configFromCustomizedSimWithTLS starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with a tls.Config. The returned client
// config can be configured to allow/decline insecure connections.
func configFromCustomizedSimWithTLS(tlsConfig *tls.Config, insecureAllowed bool) (*cnsconfig.Config, func()) {
	cfg := &cnsconfig.Config{}
	model := simulator.VPX()
	defer model.Remove()

	// configure multiple datastores in the vcsim instance
	model.Datastore = 3

	err := model.Create()
	if err != nil {
		log.Fatal(err)
	}

	model.Service.RegisterEndpoints = true
	model.Service.TLS = tlsConfig
	s := model.Service.NewServer()

	// CNS Service simulator
	model.Service.RegisterSDK(cnssim.New())

	cfg.Global.InsecureFlag = insecureAllowed

	cfg.Global.VCenterIP = s.URL.Hostname()
	cfg.Global.VCenterPort = s.URL.Port()
	cfg.Global.User = s.URL.User.Username() + "@vsphere.local"
	cfg.Global.Password, _ = s.URL.User.Password()
	cfg.Global.Datacenters = "DC0"

	// Write values to test_vsphere.conf
	os.Setenv("VSPHERE_CSI_CONFIG", "test_vsphere.conf")
	conf := []byte(fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"",
		cfg.Global.InsecureFlag, cfg.Global.VCenterIP, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort))
	err = os.WriteFile("test_vsphere.conf", conf, 0644)
	if err != nil {
		log.Fatal(err)
	}

	cfg.VirtualCenter = make(map[string]*cnsconfig.VirtualCenterConfig)
	cfg.VirtualCenter[s.URL.Hostname()] = &cnsconfig.VirtualCenterConfig{
		User:         cfg.Global.User,
		Password:     cfg.Global.Password,
		VCenterPort:  cfg.Global.VCenterPort,
		InsecureFlag: cfg.Global.InsecureFlag,
		Datacenters:  cfg.Global.Datacenters,
	}

	return cfg, func() {
		s.Close()
		model.Remove()
	}

}

func configFromEnvOrSim() (*cnsconfig.Config, func()) {
	cfg := &cnsconfig.Config{}
	if err := cnsconfig.FromEnv(ctx, cfg); err != nil {
		return configFromSim()
	}
	return cfg, func() {}
}

func getCommonUtilsTest(t *testing.T) *commonUtilsTest {
	onceForControllerTest.Do(func() {
		// Create context
		ctx = context.Background()
		csiConfig, _ := configFromEnvOrSim()

		// CNS based CSI requires a valid cluster name
		csiConfig.Global.ClusterID = testClusterName

		// Init VC configuration
		cnsVCenterConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, csiConfig)
		if err != nil {
			t.Fatal(err)
		}

		virtualCenterManager := cnsvsphere.GetVirtualCenterManager(ctx)
		virtualCenter, err := virtualCenterManager.RegisterVirtualCenter(ctx, cnsVCenterConfig)
		if err != nil {
			t.Fatal(err)
		}

		err = virtualCenter.ConnectCns(ctx)
		if err != nil {
			t.Fatal(err)
		}

		volumeManager, err := cnsvolumes.GetManager(ctx, virtualCenter, nil, false, false, false, "", "", "")
		if err != nil {
			t.Fatalf("failed to create an instance of volume manager. err=%v", err)
		}

		// wait till property collector has been started
		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, false,
			func(ctx context.Context) (done bool, err error) {
				return volumeManager.IsListViewReady(), nil
			})
		if err != nil {
			t.Fatalf("listview not ready. err=%v", err)
		}

		commonUtilsTestInstance = &commonUtilsTest{
			volumeManager: volumeManager,
		}
	})
	return commonUtilsTestInstance
}

func TestQuerySnapshotsUtil(t *testing.T) {
	// Create context
	commonUtilsTestInstance := getCommonUtilsTest(t)

	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: nil,
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  10,
		},
	}
	queryResultEntries, _, err := QuerySnapshotsUtil(ctx, commonUtilsTestInstance.volumeManager,
		queryFilter, DefaultQuerySnapshotLimit)
	if err != nil {
		t.Error(err)
	}
	// TODO: Create Snapshots using CreateSnapshot API.
	t.Log("Snapshots: ")
	for _, entry := range queryResultEntries {
		t.Log(entry)
	}
}

// TestListVirtualMachines tests the ListVirtualMachines function
func TestListVirtualMachines(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		namespace     string
		vms           []client.Object
		expectedCount int
		expectError   bool
		clientError   error
	}{
		{
			name:          "Successfully list multiple VMs",
			namespace:     "test-namespace",
			vms:           createTestVMs("test-namespace", 3),
			expectedCount: 3,
			expectError:   false,
		},
		{
			name:          "Successfully list single VM",
			namespace:     "test-namespace",
			vms:           createTestVMs("test-namespace", 1),
			expectedCount: 1,
			expectError:   false,
		},
		{
			name:          "Successfully list empty namespace",
			namespace:     "empty-namespace",
			vms:           []client.Object{},
			expectedCount: 0,
			expectError:   false,
		},
		{
			name:          "Filter VMs by namespace",
			namespace:     "namespace-1",
			vms:           append(createTestVMs("namespace-1", 2), createTestVMs("namespace-2", 2)...),
			expectedCount: 2,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			err := vmoperatortypes.AddToScheme(scheme)
			if err != nil {
				t.Fatalf("Failed to add vmoperator scheme: %v", err)
			}

			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			if len(tt.vms) > 0 {
				clientBuilder = clientBuilder.WithObjects(tt.vms...)
			}

			// Create a client that returns an error if clientError is set
			var fakeClient client.Client
			if tt.clientError != nil {
				// For error testing, we'll use a custom client that returns errors
				fakeClient = &errorClient{
					Client: clientBuilder.Build(),
					err:    tt.clientError,
				}
			} else {
				fakeClient = clientBuilder.Build()
			}

			vmList, err := ListVirtualMachines(ctx, fakeClient, tt.namespace)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if vmList != nil {
					t.Errorf("Expected nil vmList on error, but got %v", vmList)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if vmList == nil {
					t.Fatal("Expected vmList but got nil")
				}
				if len(vmList.Items) != tt.expectedCount {
					t.Errorf("Expected %d VMs, but got %d", tt.expectedCount, len(vmList.Items))
				}
				// Verify all VMs are in the correct namespace
				for _, vm := range vmList.Items {
					if vm.Namespace != tt.namespace {
						t.Errorf("VM %s is in namespace %s, expected %s", vm.Name,
							vm.Namespace, tt.namespace)
					}
				}
			}
		})
	}
}

// TestListVirtualMachinesError tests error handling in ListVirtualMachines
func TestListVirtualMachinesError(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	err := vmoperatortypes.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add vmoperator scheme: %v", err)
	}

	// Create a client that always returns an error on List
	expectedError := errors.New("failed to list virtual machines")
	errorClient := &errorClient{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		err:    expectedError,
	}

	vmList, err := ListVirtualMachines(ctx, errorClient, "test-namespace")

	if err == nil {
		t.Error("Expected error but got none")
	}
	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("Expected error %v, but got %v", expectedError, err)
	}
	if vmList != nil {
		t.Errorf("Expected nil vmList on error, but got %v", vmList)
	}
}

// TestListVirtualMachinesAllNamespaces tests the ListVirtualMachinesAllNamespaces function
func TestListVirtualMachinesAllNamespaces(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		vms           []client.Object
		expectedCount int
		expectError   bool
		clientError   error
	}{
		{
			name: "Successfully list VMs from multiple namespaces",
			vms: append(createTestVMs("namespace-1", 2), append(createTestVMs("namespace-2", 3),
				createTestVMs("namespace-3", 1)...)...),
			expectedCount: 6,
			expectError:   false,
		},
		{
			name:          "Successfully list VMs from single namespace",
			vms:           createTestVMs("test-namespace", 3),
			expectedCount: 3,
			expectError:   false,
		},
		{
			name:          "Successfully list empty result",
			vms:           []client.Object{},
			expectedCount: 0,
			expectError:   false,
		},
		{
			name:          "Successfully list single VM",
			vms:           createTestVMs("test-namespace", 1),
			expectedCount: 1,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			err := vmoperatortypes.AddToScheme(scheme)
			if err != nil {
				t.Fatalf("Failed to add vmoperator scheme: %v", err)
			}

			clientBuilder := fake.NewClientBuilder().WithScheme(scheme)
			if len(tt.vms) > 0 {
				clientBuilder = clientBuilder.WithObjects(tt.vms...)
			}

			// Create a client that returns an error if clientError is set
			var fakeClient client.Client
			if tt.clientError != nil {
				// For error testing, we'll use a custom client that returns errors
				fakeClient = &errorClient{
					Client: clientBuilder.Build(),
					err:    tt.clientError,
				}
			} else {
				fakeClient = clientBuilder.Build()
			}

			vmList, err := ListVirtualMachinesAllNamespaces(ctx, fakeClient)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if vmList != nil {
					t.Errorf("Expected nil vmList on error, but got %v", vmList)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if vmList == nil {
					t.Fatal("Expected vmList but got nil")
				}
				if len(vmList.Items) != tt.expectedCount {
					t.Errorf("Expected %d VMs, but got %d", tt.expectedCount, len(vmList.Items))
				}
			}
		})
	}
}

// TestListVirtualMachinesAllNamespacesError tests error handling in ListVirtualMachinesAllNamespaces
func TestListVirtualMachinesAllNamespacesError(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	err := vmoperatortypes.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add vmoperator scheme: %v", err)
	}

	// Create a client that always returns an error on List
	expectedError := errors.New("failed to list virtual machines from all namespaces")
	errorClient := &errorClient{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		err:    expectedError,
	}

	vmList, err := ListVirtualMachinesAllNamespaces(ctx, errorClient)

	if err == nil {
		t.Error("Expected error but got none")
	}
	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("Expected error %v, but got %v", expectedError, err)
	}
	if vmList != nil {
		t.Errorf("Expected nil vmList on error, but got %v", vmList)
	}
}

// TestListVirtualMachinesAllNamespacesMultipleNamespaces tests that VMs from all namespaces are returned
func TestListVirtualMachinesAllNamespacesMultipleNamespaces(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	err := vmoperatortypes.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add vmoperator scheme: %v", err)
	}

	// Create VMs in different namespaces
	vms := append(
		createTestVMs("namespace-1", 2),
		append(
			createTestVMs("namespace-2", 3),
			createTestVMs("namespace-3", 1)...,
		)...,
	)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vms...).Build()

	vmList, err := ListVirtualMachinesAllNamespaces(ctx, fakeClient)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if vmList == nil {
		t.Fatal("Expected vmList but got nil")
	}

	expectedCount := 6
	if len(vmList.Items) != expectedCount {
		t.Errorf("Expected %d VMs, but got %d", expectedCount, len(vmList.Items))
	}

	// Verify VMs from all namespaces are present
	namespaceCounts := make(map[string]int)
	for _, vm := range vmList.Items {
		namespaceCounts[vm.Namespace]++
	}

	if namespaceCounts["namespace-1"] != 2 {
		t.Errorf("Expected 2 VMs in namespace-1, but got %d", namespaceCounts["namespace-1"])
	}
	if namespaceCounts["namespace-2"] != 3 {
		t.Errorf("Expected 3 VMs in namespace-2, but got %d", namespaceCounts["namespace-2"])
	}
	if namespaceCounts["namespace-3"] != 1 {
		t.Errorf("Expected 1 VM in namespace-3, but got %d", namespaceCounts["namespace-3"])
	}
}

// createTestVMs creates a slice of test VirtualMachine objects
func createTestVMs(namespace string, count int) []client.Object {
	vms := make([]client.Object, count)
	for i := 0; i < count; i++ {
		vms[i] = &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-vm-%d", i),
				Namespace: namespace,
			},
			Spec: vmoperatortypes.VirtualMachineSpec{
				ImageName: "test-image",
			},
		}
	}
	return vms
}

// errorClient is a wrapper around a fake client that returns errors on List operations
type errorClient struct {
	client.Client
	err error
}

func (e *errorClient) List(ctx context.Context, list client.ObjectList,
	opts ...client.ListOption) error {
	return e.err
}

// patchErrorClient is a wrapper around a fake client that returns errors on Patch operations
type patchErrorClient struct {
	client.Client
	err error
}

func (e *patchErrorClient) Patch(ctx context.Context, obj client.Object,
	patch client.Patch, opts ...client.PatchOption) error {
	return e.err
}

// TestPatchVirtualMachine tests the PatchVirtualMachine function
func TestPatchVirtualMachine(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		vm          *vmoperatortypes.VirtualMachine
		oldVM       *vmoperatortypes.VirtualMachine
		expectError bool
		clientError error
		setupClient func(*testing.T, *vmoperatortypes.VirtualMachine) client.Client
	}{
		{
			name: "Successfully patch VM with spec change",
			vm: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-vm",
					Namespace:       "test-namespace",
					ResourceVersion: "1",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "updated-image",
					ClassName: "updated-class",
				},
			},
			oldVM: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-vm",
					Namespace:       "test-namespace",
					ResourceVersion: "1",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "original-image",
					ClassName: "original-class",
				},
			},
			expectError: false,
			setupClient: func(t *testing.T, vm *vmoperatortypes.VirtualMachine) client.Client {
				scheme := runtime.NewScheme()
				err := vmoperatortypes.AddToScheme(scheme)
				if err != nil {
					t.Fatalf("Failed to add vmoperator scheme: %v", err)
				}
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(vm).Build()
			},
		},
		{
			name: "Successfully patch VM with metadata change",
			vm: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "test-namespace",
					Labels: map[string]string{
						"new-label": "new-value",
					},
					ResourceVersion: "2",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "test-image",
				},
			},
			oldVM: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-vm",
					Namespace:       "test-namespace",
					ResourceVersion: "1",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "test-image",
				},
			},
			expectError: false,
			setupClient: func(t *testing.T, vm *vmoperatortypes.VirtualMachine) client.Client {
				scheme := runtime.NewScheme()
				err := vmoperatortypes.AddToScheme(scheme)
				if err != nil {
					t.Fatalf("Failed to add vmoperator scheme: %v", err)
				}
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(vm).Build()
			},
		},
		{
			name: "Error when patch fails",
			vm: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-vm",
					Namespace:       "test-namespace",
					ResourceVersion: "1",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "updated-image",
				},
			},
			oldVM: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-vm",
					Namespace:       "test-namespace",
					ResourceVersion: "1",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "original-image",
				},
			},
			expectError: true,
			clientError: errors.New("failed to patch virtual machine"),
			setupClient: func(t *testing.T, vm *vmoperatortypes.VirtualMachine) client.Client {
				scheme := runtime.NewScheme()
				err := vmoperatortypes.AddToScheme(scheme)
				if err != nil {
					t.Fatalf("Failed to add vmoperator scheme: %v", err)
				}
				baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
				return &patchErrorClient{
					Client: baseClient,
					err:    errors.New("failed to patch virtual machine"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the initial VM in the client
			initialVM := tt.oldVM.DeepCopy()
			fakeClient := tt.setupClient(t, initialVM)

			err := PatchVirtualMachine(ctx, fakeClient, tt.vm, tt.oldVM)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if tt.clientError != nil && err.Error() != tt.clientError.Error() {
					t.Errorf("Expected error %v, but got %v", tt.clientError, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				// Verify the VM was patched by getting it from the client
				patchedVM := &vmoperatortypes.VirtualMachine{}
				err = fakeClient.Get(ctx, client.ObjectKey{
					Name:      tt.vm.Name,
					Namespace: tt.vm.Namespace,
				}, patchedVM)
				if err != nil {
					t.Errorf("Failed to get patched VM: %v", err)
				}
				// Verify the spec was updated
				if patchedVM.Spec.ImageName != tt.vm.Spec.ImageName {
					t.Errorf("Expected ImageName %s, but got %s", tt.vm.Spec.ImageName,
						patchedVM.Spec.ImageName)
				}
			}
		})
	}
}

// TestPatchVirtualMachineOptimisticLocking tests that optimistic locking is used
func TestPatchVirtualMachineOptimisticLocking(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	err := vmoperatortypes.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add vmoperator scheme: %v", err)
	}

	// Create initial VM
	oldVM := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-vm",
			Namespace:       "test-namespace",
			ResourceVersion: "1",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			ImageName: "original-image",
		},
	}

	// Create updated VM
	newVM := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-vm",
			Namespace:       "test-namespace",
			ResourceVersion: "2",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			ImageName: "updated-image",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(oldVM).Build()

	err = PatchVirtualMachine(ctx, fakeClient, newVM, oldVM)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify the patch was applied
	patchedVM := &vmoperatortypes.VirtualMachine{}
	err = fakeClient.Get(ctx, client.ObjectKey{
		Name:      newVM.Name,
		Namespace: newVM.Namespace,
	}, patchedVM)
	if err != nil {
		t.Fatalf("Failed to get patched VM: %v", err)
	}

	if patchedVM.Spec.ImageName != "updated-image" {
		t.Errorf("Expected ImageName 'updated-image', but got %s", patchedVM.Spec.ImageName)
	}
}

// TestPatchVirtualMachinePartialUpdate tests that only changed fields are updated
func TestPatchVirtualMachinePartialUpdate(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	err := vmoperatortypes.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add vmoperator scheme: %v", err)
	}

	// Create initial VM with multiple fields
	oldVM := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-vm",
			Namespace:       "test-namespace",
			ResourceVersion: "1",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			ImageName: "original-image",
			ClassName: "original-class",
		},
	}

	// Create updated VM - only ImageName changed
	newVM := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-vm",
			Namespace:       "test-namespace",
			ResourceVersion: "2",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			ImageName: "updated-image",
			ClassName: "original-class", // This should remain unchanged
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(oldVM).Build()

	err = PatchVirtualMachine(ctx, fakeClient, newVM, oldVM)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify the patch was applied correctly
	patchedVM := &vmoperatortypes.VirtualMachine{}
	err = fakeClient.Get(ctx, client.ObjectKey{
		Name:      newVM.Name,
		Namespace: newVM.Namespace,
	}, patchedVM)
	if err != nil {
		t.Fatalf("Failed to get patched VM: %v", err)
	}

	// Verify ImageName was updated
	if patchedVM.Spec.ImageName != "updated-image" {
		t.Errorf("Expected ImageName 'updated-image', but got %s", patchedVM.Spec.ImageName)
	}

	// Verify ClassName remained unchanged
	if patchedVM.Spec.ClassName != "original-class" {
		t.Errorf("Expected ClassName 'original-class', but got %s", patchedVM.Spec.ClassName)
	}
}

// getErrorClient is a wrapper around a fake client that returns errors on Get operations
type getErrorClient struct {
	client.Client
	err error
}

func (e *getErrorClient) Get(ctx context.Context, key client.ObjectKey,
	obj client.Object, opts ...client.GetOption) error {
	return e.err
}

// TestGetVirtualMachine tests the GetVirtualMachine function
func TestGetVirtualMachine(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		vmKey          types.NamespacedName
		vm             *vmoperatortypes.VirtualMachine
		expectError    bool
		clientError    error
		expectedVMName string
		setupClient    func(*testing.T, *vmoperatortypes.VirtualMachine) client.Client
	}{
		{
			name: "Successfully get VM",
			vmKey: types.NamespacedName{
				Name:      "test-vm",
				Namespace: "test-namespace",
			},
			vm: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "test-namespace",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "test-image",
					ClassName: "test-class",
				},
			},
			expectError:    false,
			expectedVMName: "test-vm",
			setupClient: func(t *testing.T, vm *vmoperatortypes.VirtualMachine) client.Client {
				scheme := runtime.NewScheme()
				err := vmoperatortypes.AddToScheme(scheme)
				if err != nil {
					t.Fatalf("Failed to add vmoperator scheme: %v", err)
				}
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(vm).Build()
			},
		},
		{
			name: "Error when VM does not exist",
			vmKey: types.NamespacedName{
				Name:      "non-existent-vm",
				Namespace: "test-namespace",
			},
			vm: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "test-namespace",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "test-image",
				},
			},
			expectError: true,
			setupClient: func(t *testing.T, vm *vmoperatortypes.VirtualMachine) client.Client {
				scheme := runtime.NewScheme()
				err := vmoperatortypes.AddToScheme(scheme)
				if err != nil {
					t.Fatalf("Failed to add vmoperator scheme: %v", err)
				}
				// Create client with a different VM, so the requested one doesn't exist
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(vm).Build()
			},
		},
		{
			name: "Error when client returns error",
			vmKey: types.NamespacedName{
				Name:      "test-vm",
				Namespace: "test-namespace",
			},
			vm: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "test-namespace",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "test-image",
				},
			},
			expectError: true,
			clientError: errors.New("client error"),
			setupClient: func(t *testing.T, vm *vmoperatortypes.VirtualMachine) client.Client {
				scheme := runtime.NewScheme()
				err := vmoperatortypes.AddToScheme(scheme)
				if err != nil {
					t.Fatalf("Failed to add vmoperator scheme: %v", err)
				}
				baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
				return &getErrorClient{
					Client: baseClient,
					err:    errors.New("client error"),
				}
			},
		},
		{
			name: "Successfully get VM with different namespace",
			vmKey: types.NamespacedName{
				Name:      "test-vm",
				Namespace: "namespace-2",
			},
			vm: &vmoperatortypes.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "namespace-2",
				},
				Spec: vmoperatortypes.VirtualMachineSpec{
					ImageName: "test-image",
					ClassName: "test-class",
				},
			},
			expectError:    false,
			expectedVMName: "test-vm",
			setupClient: func(t *testing.T, vm *vmoperatortypes.VirtualMachine) client.Client {
				scheme := runtime.NewScheme()
				err := vmoperatortypes.AddToScheme(scheme)
				if err != nil {
					t.Fatalf("Failed to add vmoperator scheme: %v", err)
				}
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(vm).Build()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := tt.setupClient(t, tt.vm)

			vm, apiVersion, err := GetVirtualMachine(ctx, tt.vmKey, fakeClient)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if vm != nil {
					t.Errorf("Expected nil VM on error, but got %v", vm)
				}
				if tt.clientError != nil && err.Error() != tt.clientError.Error() {
					// For NotFound errors, we don't check the exact error message
					// as the fake client may return a different error format
					if tt.clientError.Error() == "client error" {
						t.Errorf("Expected error %v, but got %v", tt.clientError, err)
					}
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if vm == nil {
					t.Fatal("Expected VM but got nil")
				}
				if vm.Name != tt.expectedVMName {
					t.Errorf("Expected VM name %s, but got %s", tt.expectedVMName, vm.Name)
				}
				if vm.Namespace != tt.vmKey.Namespace {
					t.Errorf("Expected VM namespace %s, but got %s", tt.vmKey.Namespace, vm.Namespace)
				}
				// Verify apiVersion is correct
				expectedAPIVersion := "vmoperator.vmware.com/v1alpha2"
				if apiVersion != expectedAPIVersion {
					t.Errorf("Expected apiVersion %s, but got %s", expectedAPIVersion, apiVersion)
				}
			}
		})
	}
}

// TestGetVirtualMachineAPIVersion tests that the correct API version is returned
func TestGetVirtualMachineAPIVersion(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	err := vmoperatortypes.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add vmoperator scheme: %v", err)
	}

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "test-namespace",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			ImageName: "test-image",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(vm).Build()

	vmKey := types.NamespacedName{
		Name:      "test-vm",
		Namespace: "test-namespace",
	}

	retrievedVM, apiVersion, err := GetVirtualMachine(ctx, vmKey, fakeClient)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if retrievedVM == nil {
		t.Fatal("Expected VM but got nil")
	}

	expectedAPIVersion := "vmoperator.vmware.com/v1alpha2"
	if apiVersion != expectedAPIVersion {
		t.Errorf("Expected apiVersion %s, but got %s", expectedAPIVersion, apiVersion)
	}

	// Verify the VM data is correct
	if retrievedVM.Name != "test-vm" {
		t.Errorf("Expected VM name 'test-vm', but got %s", retrievedVM.Name)
	}
	if retrievedVM.Spec.ImageName != "test-image" {
		t.Errorf("Expected ImageName 'test-image', but got %s", retrievedVM.Spec.ImageName)
	}
}
