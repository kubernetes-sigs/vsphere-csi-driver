package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	vmoperatorv1alpha1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmoperatorv1alpha2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoperatorv1alpha3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	vmoperatorv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	cnssim "github.com/vmware/govmomi/cns/simulator"
	"github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"
	_ "github.com/vmware/govmomi/vapi/simulator"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	cnsvolumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
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

		volumeManager, err := cnsvolumes.GetManager(ctx, virtualCenter, nil, false, false, false, "")
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

	queryFilter := types.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: nil,
		Cursor: &types.CnsCursor{
			Offset: 0,
			Limit:  10,
		},
	}
	queryResultEntries, _, err := QuerySnapshotsUtil(ctx, commonUtilsTestInstance.volumeManager, queryFilter,
		DefaultQuerySnapshotLimit)
	if err != nil {
		t.Error(err)
	}
	// TODO: Create Snapshots using CreateSnapshot API.
	t.Log("Snapshots: ")
	for _, entry := range queryResultEntries {
		t.Log(entry)
	}
}

func TestListVirtualMachines(t *testing.T) {
	getLatestCRDVersionOriginal := getLatestCRDVersion
	defer func() {
		getLatestCRDVersion = getLatestCRDVersionOriginal
	}()

	t.Run("WhenLatestCRDVersionIsNotAvailable", func(tt *testing.T) {
		// Setup
		getLatestCRDVersion = func(ctx context.Context, crdName string) (string, error) {
			return "", fmt.Errorf("CRD version not available")
		}

		// Execute
		_, err := ListVirtualMachines(context.Background(), fake.NewFakeClient(), "")

		// Assert
		assert.NotNil(tt, err)
	})

	t.Run("WhenLatestCRDVersionIsV1Alpha1", func(tt *testing.T) {
		getLatestCRDVersion = func(ctx context.Context, crdName string) (string, error) {
			return "v1alpha1", nil
		}
		tt.Run("WhenListFails", func(ttt *testing.T) {
			// Setup
			clientBuilder := fake.NewClientBuilder()
			clientBuilder.WithInterceptorFuncs(
				interceptor.Funcs{
					List: func(ctx context.Context, client client.WithWatch, list client.ObjectList,
						opts ...client.ListOption) error {
						return fmt.Errorf("failing list for testing purposes")
					}})

			// Execute
			_, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), "")

			// Assert
			assert.NotNil(ttt, err)
		})
		tt.Run("WhenListSucceeds", func(ttt *testing.T) {
			// Setup
			namespace := &v1.Namespace{
				TypeMeta: metav1.TypeMeta{
					Kind: "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace",
				},
				Spec: v1.NamespaceSpec{
					Finalizers: []v1.FinalizerName{
						v1.FinalizerKubernetes,
					},
				},
				Status: v1.NamespaceStatus{
					Phase: v1.NamespaceActive,
				},
			}
			otherNamespace := &v1.Namespace{
				TypeMeta: metav1.TypeMeta{
					Kind: "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "other-namespace",
				},
				Spec: v1.NamespaceSpec{
					Finalizers: []v1.FinalizerName{
						v1.FinalizerKubernetes,
					},
				},
				Status: v1.NamespaceStatus{
					Phase: v1.NamespaceActive,
				},
			}
			vm1 := &vmoperatorv1alpha1.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			vm2 := &vmoperatorv1alpha1.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			vm3 := &vmoperatorv1alpha1.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm3",
					Namespace: "other-namespace",
				},
			}
			clientBuilder := fake.NewClientBuilder()
			scheme := runtime.NewScheme()
			clientBuilder = registerSchemes(context.Background(), clientBuilder, scheme, runtime.SchemeBuilder{
				v1.AddToScheme,
				vmoperatorv1alpha1.AddToScheme,
			})
			clientBuilder.WithRuntimeObjects(namespace, otherNamespace, vm1, vm2, vm3)
			v1Alpha4VM1 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			v1Alpha4VM2 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			exp := vmoperatorv1alpha5.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{},
				ListMeta: metav1.ListMeta{},
				Items: []vmoperatorv1alpha5.VirtualMachine{
					v1Alpha4VM1,
					v1Alpha4VM2,
				},
			}

			// Execute
			actual, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), namespace.Name)

			// Assert
			assert.Nil(tt, err)
			assert.NotNil(tt, actual)
			assert.True(tt, compareVirtualMachineLists(exp, *actual))
		})
	})

	t.Run("WhenLatestCRDVersionIsV1Alpha2", func(tt *testing.T) {
		getLatestCRDVersion = func(ctx context.Context, crdName string) (string, error) {
			return "v1alpha2", nil
		}
		tt.Run("WhenListFails", func(ttt *testing.T) {
			// Setup
			clientBuilder := fake.NewClientBuilder()
			clientBuilder.WithInterceptorFuncs(
				interceptor.Funcs{
					List: func(ctx context.Context, client client.WithWatch, list client.ObjectList,
						opts ...client.ListOption) error {
						return fmt.Errorf("failing list for testing purposes")
					}})

			// Execute
			_, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), "")

			// Assert
			assert.NotNil(ttt, err)
		})
		tt.Run("WhenListSucceeds", func(ttt *testing.T) {
			// Setup
			namespace := &v1.Namespace{
				TypeMeta: metav1.TypeMeta{
					Kind: "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace",
				},
				Spec: v1.NamespaceSpec{
					Finalizers: []v1.FinalizerName{
						v1.FinalizerKubernetes,
					},
				},
				Status: v1.NamespaceStatus{
					Phase: v1.NamespaceActive,
				},
			}
			vm1 := &vmoperatorv1alpha2.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha2",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			vm2 := &vmoperatorv1alpha2.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha2",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			clientBuilder := fake.NewClientBuilder()
			scheme := runtime.NewScheme()
			clientBuilder = registerSchemes(context.Background(), clientBuilder, scheme, runtime.SchemeBuilder{
				v1.AddToScheme,
				vmoperatorv1alpha2.AddToScheme,
			})
			clientBuilder.WithRuntimeObjects(namespace, vm1, vm2)
			v1Alpha4VM1 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			v1Alpha4VM2 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			exp := vmoperatorv1alpha5.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{},
				ListMeta: metav1.ListMeta{},
				Items: []vmoperatorv1alpha5.VirtualMachine{
					v1Alpha4VM1,
					v1Alpha4VM2,
				},
			}

			// Execute
			actual, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), namespace.Name)

			// Assert
			assert.Nil(tt, err)
			assert.NotNil(tt, actual)
			assert.True(tt, compareVirtualMachineLists(exp, *actual))
		})
	})

	t.Run("WhenLatestCRDVersionIsV1Alpha3", func(tt *testing.T) {
		getLatestCRDVersion = func(ctx context.Context, crdName string) (string, error) {
			return "v1alpha3", nil
		}
		tt.Run("WhenListFails", func(ttt *testing.T) {
			// Setup
			clientBuilder := fake.NewClientBuilder()
			clientBuilder.WithInterceptorFuncs(
				interceptor.Funcs{
					List: func(ctx context.Context, client client.WithWatch, list client.ObjectList,
						opts ...client.ListOption) error {
						return fmt.Errorf("failing list for testing purposes")
					}})

			// Execute
			_, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), "")

			// Assert
			assert.NotNil(ttt, err)
		})
		tt.Run("WhenListSucceeds", func(ttt *testing.T) {
			// Setup
			namespace := &v1.Namespace{
				TypeMeta: metav1.TypeMeta{
					Kind: "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace",
				},
				Spec: v1.NamespaceSpec{
					Finalizers: []v1.FinalizerName{
						v1.FinalizerKubernetes,
					},
				},
				Status: v1.NamespaceStatus{
					Phase: v1.NamespaceActive,
				},
			}
			vm1 := &vmoperatorv1alpha3.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha3",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			vm2 := &vmoperatorv1alpha3.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha3",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			clientBuilder := fake.NewClientBuilder()
			scheme := runtime.NewScheme()
			clientBuilder = registerSchemes(context.Background(), clientBuilder, scheme, runtime.SchemeBuilder{
				v1.AddToScheme,
				vmoperatorv1alpha3.AddToScheme,
			})
			clientBuilder.WithRuntimeObjects(namespace, vm1, vm2)
			v1Alpha4VM1 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			v1Alpha4VM2 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			exp := vmoperatorv1alpha5.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{},
				ListMeta: metav1.ListMeta{},
				Items: []vmoperatorv1alpha5.VirtualMachine{
					v1Alpha4VM1,
					v1Alpha4VM2,
				},
			}

			// Execute
			actual, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), namespace.Name)

			// Assert
			assert.Nil(tt, err)
			assert.NotNil(tt, actual)
			assert.True(tt, compareVirtualMachineLists(exp, *actual))
		})
	})
	t.Run("WhenLatestCRDVersionIsV1Alpha4", func(tt *testing.T) {
		getLatestCRDVersion = func(ctx context.Context, crdName string) (string, error) {
			return "v1alpha4", nil
		}
		tt.Run("WhenListFails", func(ttt *testing.T) {
			// Setup
			clientBuilder := fake.NewClientBuilder()
			clientBuilder.WithInterceptorFuncs(
				interceptor.Funcs{
					List: func(ctx context.Context, client client.WithWatch, list client.ObjectList,
						opts ...client.ListOption) error {
						return fmt.Errorf("failing list for testing purposes")
					}})

			// Execute
			_, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), "")

			// Assert
			assert.NotNil(ttt, err)
		})
		tt.Run("WhenListSucceeds", func(ttt *testing.T) {
			// Setup
			namespace := &v1.Namespace{
				TypeMeta: metav1.TypeMeta{
					Kind: "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace",
				},
				Spec: v1.NamespaceSpec{
					Finalizers: []v1.FinalizerName{
						v1.FinalizerKubernetes,
					},
				},
				Status: v1.NamespaceStatus{
					Phase: v1.NamespaceActive,
				},
			}
			vm1 := &vmoperatorv1alpha4.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha4",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			vm2 := &vmoperatorv1alpha4.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha4",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			clientBuilder := fake.NewClientBuilder()
			scheme := runtime.NewScheme()
			clientBuilder = registerSchemes(context.Background(), clientBuilder, scheme, runtime.SchemeBuilder{
				v1.AddToScheme,
				vmoperatorv1alpha4.AddToScheme,
			})
			clientBuilder.WithRuntimeObjects(namespace, vm1, vm2)
			v1Alpha4VM1 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			v1Alpha4VM2 := vmoperatorv1alpha5.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			exp := vmoperatorv1alpha5.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{},
				ListMeta: metav1.ListMeta{},
				Items: []vmoperatorv1alpha5.VirtualMachine{
					v1Alpha4VM1,
					v1Alpha4VM2,
				},
			}

			// Execute
			actual, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), namespace.Name)

			// Assert
			assert.Nil(tt, err)
			assert.NotNil(tt, actual)
			assert.True(tt, compareVirtualMachineLists(exp, *actual))
		})
	})
	t.Run("WhenLatestCRDVersionIsV1Alpha5OrAbove", func(tt *testing.T) {
		getLatestCRDVersion = func(ctx context.Context, crdName string) (string, error) {
			return "v1alpha5", nil
		}
		tt.Run("WhenListFails", func(ttt *testing.T) {
			// Setup
			clientBuilder := fake.NewClientBuilder()
			clientBuilder.WithInterceptorFuncs(
				interceptor.Funcs{
					List: func(ctx context.Context, client client.WithWatch, list client.ObjectList,
						opts ...client.ListOption) error {
						return fmt.Errorf("failing list for testing purposes")
					}})

			// Execute
			_, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), "")

			// Assert
			assert.NotNil(ttt, err)
		})
		tt.Run("WhenListSucceeds", func(ttt *testing.T) {
			// Setup
			namespace := &v1.Namespace{
				TypeMeta: metav1.TypeMeta{
					Kind: "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace",
				},
				Spec: v1.NamespaceSpec{
					Finalizers: []v1.FinalizerName{
						v1.FinalizerKubernetes,
					},
				},
				Status: v1.NamespaceStatus{
					Phase: v1.NamespaceActive,
				},
			}
			vm1 := &vmoperatorv1alpha5.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha5",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm1",
					Namespace: namespace.Name,
				},
			}
			vm2 := &vmoperatorv1alpha5.VirtualMachine{
				TypeMeta: metav1.TypeMeta{
					Kind:       "VirtualMachine",
					APIVersion: "vmoperator.vmware.com/v1alpha5",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vm2",
					Namespace: namespace.Name,
				},
			}
			clientBuilder := fake.NewClientBuilder()
			scheme := runtime.NewScheme()
			clientBuilder = registerSchemes(context.Background(), clientBuilder, scheme, runtime.SchemeBuilder{
				v1.AddToScheme,
				vmoperatorv1alpha5.AddToScheme,
			})
			clientBuilder.WithRuntimeObjects(namespace, vm1, vm2)
			exp := vmoperatorv1alpha5.VirtualMachineList{
				TypeMeta: metav1.TypeMeta{},
				ListMeta: metav1.ListMeta{},
				Items: []vmoperatorv1alpha5.VirtualMachine{
					*vm1,
					*vm2,
				},
			}

			// Execute
			actual, err := ListVirtualMachines(context.Background(), clientBuilder.Build(), namespace.Name)

			// Assert
			assert.Nil(tt, err)
			assert.NotNil(tt, actual)
			assert.True(tt, compareVirtualMachineLists(exp, *actual))
		})
	})
}

func registerSchemes(ctx context.Context, clientBuilder *fake.ClientBuilder, scheme *runtime.Scheme,
	schemeBuilder runtime.SchemeBuilder) *fake.ClientBuilder {
	l := logger.GetLogger(ctx)
	if err := schemeBuilder.AddToScheme(scheme); err != nil {
		l.Fatalf("Failed to add scheme: %v", err)
	}

	clientBuilder.WithScheme(scheme)
	return clientBuilder
}

func compareVirtualMachineLists(exp, actual vmoperatorv1alpha5.VirtualMachineList) bool {
	// since the list output may not be in the same order, we will compare the items
	// using brute force.
	if len(exp.Items) != len(actual.Items) {
		return false
	}

	for _, expItem := range exp.Items {
		found := false
		for _, actualItem := range actual.Items {
			if expItem.Name == actualItem.Name && expItem.Namespace == actualItem.Namespace {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}
