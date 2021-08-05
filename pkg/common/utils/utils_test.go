package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	cnssim "github.com/vmware/govmomi/cns/simulator"
	"github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"
	cnsvolumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
)

const (
	testClusterName = "test-cluster"
)

var (
	csiConfig            *cnsconfig.Config
	ctx                  context.Context
	cnsVCenterConfig     *cnsvsphere.VirtualCenterConfig
	err                  error
	virtualCenterManager cnsvsphere.VirtualCenterManager
	virtualCenter        *cnsvsphere.VirtualCenter
	volumeManager        cnsvolumes.Manager
	cancel               context.CancelFunc
)

// configFromSim starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with an empty tls.Config.
func configFromSim() (*cnsconfig.Config, func()) {
	return configFromSimWithTLS(new(tls.Config), true)
}

// configFromSimWithTLS starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with a tls.Config. The returned client
// config can be configured to allow/decline insecure connections.
func configFromSimWithTLS(tlsConfig *tls.Config, insecureAllowed bool) (*cnsconfig.Config, func()) {
	cfg := &cnsconfig.Config{}
	model := simulator.VPX()
	defer model.Remove()

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
	cfg.Global.User = s.URL.User.Username()
	cfg.Global.Password, _ = s.URL.User.Password()
	cfg.Global.Datacenters = "DC0"

	// Write values to test_vsphere.conf
	os.Setenv("VSPHERE_CSI_CONFIG", "test_vsphere.conf")
	conf := []byte(fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"",
		cfg.Global.InsecureFlag, cfg.Global.VCenterIP, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort))
	err = ioutil.WriteFile("test_vsphere.conf", conf, 0644)
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

func TestQuerySnapshotsUtil(t *testing.T) {
	// Create context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	t.Log("TestQuerySnapshotsUtil: start")
	var cleanup func()
	csiConfig, cleanup = configFromEnvOrSim()
	defer cleanup()

	// CNS based CSI requires a valid cluster name
	csiConfig.Global.ClusterID = testClusterName

	// Init VC configuration
	cnsVCenterConfig, err = cnsvsphere.GetVirtualCenterConfig(ctx, csiConfig)
	if err != nil {
		t.Errorf("failed to get virtualCenter. err=%v", err)
		t.Fatal(err)
	}

	virtualCenterManager = cnsvsphere.GetVirtualCenterManager(ctx)

	virtualCenter, err = virtualCenterManager.RegisterVirtualCenter(ctx, cnsVCenterConfig)
	if err != nil {
		t.Fatal(err)
	}

	err = virtualCenter.ConnectCns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if virtualCenter != nil {
			err = virtualCenter.Disconnect(ctx)
			if err != nil {
				t.Error(err)
			}
		}
	}()

	volumeManager = cnsvolumes.GetManager(ctx, virtualCenter, nil, false)
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
