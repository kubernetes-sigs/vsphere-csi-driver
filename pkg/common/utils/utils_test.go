package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	cnssim "github.com/vmware/govmomi/cns/simulator"
	"github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"

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
	config  *cnsconfig.Config
	vcenter *cnsvsphere.VirtualCenter
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
		commonUtilsTestInstance = &commonUtilsTest{
			config:  csiConfig,
			vcenter: virtualCenter,
		}
	})
	return commonUtilsTestInstance
}

func TestQuerySnapshotsUtil(t *testing.T) {
	// Create context
	commonUtilsTestInstance := getCommonUtilsTest(t)

	volumeManager, err := cnsvolumes.GetManager(ctx, commonUtilsTestInstance.vcenter, nil, false, false, false, "")
	if err != nil {
		t.Fatalf("failed to create an instance of volume manager. err=%v", err)
	}

	queryFilter := types.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: nil,
		Cursor: &types.CnsCursor{
			Offset: 0,
			Limit:  10,
		},
	}
	queryResultEntries, _, err := QuerySnapshotsUtil(ctx, volumeManager, queryFilter, DefaultQuerySnapshotLimit)
	if err != nil {
		t.Error(err)
	}
	//TODO: Create Snapshots using CreateSnapshot API.
	t.Log("Snapshots: ")
	for _, entry := range queryResultEntries {
		t.Log(entry)
	}
}
