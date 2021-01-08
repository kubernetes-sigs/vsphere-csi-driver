/*
Copyright 2021 The Kubernetes Authors.

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

package storagepool

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"

	cnssim "github.com/vmware/govmomi/cns/simulator"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	pbmsim "github.com/vmware/govmomi/pbm/simulator"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"

	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	testclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/homedir"
	spconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	spv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/storagepool/cns/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var (
	sharedDatastoreManagedObject *mo.Datastore
	spControllerTest             *SpController
	scWatchCntlrTest             *StorageClassWatch
	freeSpaceStr                 = "12345678900"
	capacityStr                  = "98765432100"
	fakeSPName                   = "storagepool-samplesp"
)

type metadataSyncInformer struct {
	k8sInformerManager *k8s.InformerManager
}

// setUpInitStoragePoolService Initialises storagepool service
func setUpInitStoragePoolService(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string) error {
	log := logger.GetLogger(ctx)
	log.Infof("Initializing Storage Pool Service for unit tests")

	cfg, err := spconfig.GetConfig()
	if err != nil {
		log.Errorf("Failed to get kubernetes config. Err: %+v", err)
		return err
	}

	kubecfgPath := ""
	if flag.Lookup("kubeconfig") != nil {
		kubecfgPath = flag.Lookup("kubeconfig").Value.(flag.Getter).Get().(string)
		if kubecfgPath == "" {
			if home := homedir.HomeDir(); home != "" {
				flag.Set("kubeconfig", filepath.Join(home, ".kube", "config"))
			}
		}
	}

	// Create StoragePool CRD
	crdKind := reflect.TypeOf(spv1alpha1.StoragePool{}).Name()
	crdSingular := "storagepool"
	crdPlural := "storagepools"
	crdName := crdPlural + "." + spv1alpha1.SchemeGroupVersion.Group
	err = k8s.CreateCustomResourceDefinitionFromSpec(ctx, crdName, crdSingular, crdPlural,
		crdKind, spv1alpha1.SchemeGroupVersion.Group, spv1alpha1.SchemeGroupVersion.Version, apiextensionsv1beta1.ClusterScoped)
	if err != nil {
		log.Errorf("Failed to create %q CRD. Err: %+v", crdKind, err)
		return err
	}

	err = vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Failed to connect to SPBM service. Err: %+v", err)
		return err
	}

	// Start the services
	spControllerTest, err = newSPController(vc, clusterID)
	if err != nil {
		log.Errorf("Failed starting StoragePool controller. Err: %+v", err)
		return err
	}

	scWatchCntlrTest, err = startStorageClassWatch(ctx, spControllerTest, cfg)
	if err != nil {
		log.Errorf("Failed starting the Storageclass watch. Err: %+v", err)
		return err
	}

	defaultStoragePoolServiceLock.Lock()
	defer defaultStoragePoolServiceLock.Unlock()
	defaultStoragePoolService.spController = spControllerTest
	defaultStoragePoolService.scWatchCntlr = scWatchCntlrTest
	defaultStoragePoolService.clusterID = clusterID

	startPropertyCollectorListener(ctx)

	log.Infof("Done initializing Storage Pool Service for unit tests")
	return nil
}

func configFromSim() (*config.Config, func()) {
	return configFromSimWithTLS(new(tls.Config), true)
}

// configFromSimWithTLS starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with a tls.Config. The returned client
// config can be configured to allow/decline insecure connections.
func configFromSimWithTLS(tlsConfig *tls.Config, insecureAllowed bool) (*config.Config, func()) {
	cfg := &config.Config{}
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

	// PBM Service simulator
	model.Service.RegisterSDK(pbmsim.New())
	cfg.Global.InsecureFlag = insecureAllowed

	cfg.Global.VCenterIP = s.URL.Hostname()
	cfg.Global.VCenterPort = s.URL.Port()
	cfg.Global.User = s.URL.User.Username()
	cfg.Global.Password, _ = s.URL.User.Password()
	cfg.Global.Datacenters = "DC0"

	cfg.VirtualCenter = make(map[string]*config.VirtualCenterConfig)
	cfg.VirtualCenter[s.URL.Hostname()] = &config.VirtualCenterConfig{
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

func configFromEnvOrSim() (*config.Config, func()) {
	cfg := &config.Config{}
	if err := config.FromEnv(context.TODO(), cfg); err != nil {
		return configFromSim()
	}
	return cfg, func() {}
}

func getFakeDatastores(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {

	var sharedDatastoreURL string
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		sharedDatastoreURL = v
	} else {
		sharedDatastoreURL = simulator.Map.Any("Datastore").(*simulator.Datastore).Info.GetDatastoreInfo().Url
	}

	var datacenterName string
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		datacenterName = v
	} else {
		datacenterName = simulator.Map.Any("Datacenter").(*simulator.Datacenter).Name
	}

	finder := find.NewFinder(vc.Client.Client, false)
	dc, _ := finder.Datacenter(ctx, datacenterName)
	finder.SetDatacenter(dc)
	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}

	for _, dsMo := range dsMoList {
		if dsMo.Info.GetDatastoreInfo().Url == sharedDatastoreURL {
			sharedDatastoreManagedObject = &dsMo
		}
	}
	if sharedDatastoreManagedObject == nil {
		return nil, nil, fmt.Errorf("Failed to get shared datastores")
	}
	return []*cnsvsphere.DatastoreInfo{
		{
			Datastore: &cnsvsphere.Datastore{
				Datastore:  object.NewDatastore(dc.Client(), sharedDatastoreManagedObject.Reference()),
				Datacenter: nil},
			Info: sharedDatastoreManagedObject.Info.GetDatastoreInfo(),
		},
	}, nil, nil
}

// returns an accessible node where the ESX host is not in MM
func findFakeAccessibleNodes(_ context.Context, _ *object.Datastore,
	_ string, _ *vim25.Client) (map[string]bool, error) {
	nodes := map[string]bool{"wdc-node-1": false}
	return nodes, nil
}

// returns an accessible node where the ESX host is n MM
func findFakeMMNodes(_ context.Context, _ *object.Datastore,
	_ string, _ *vim25.Client) (map[string]bool, error) {
	nodes := map[string]bool{"wdc-node-1": true}
	return nodes, nil
}

// returns zero accessible nodes
func findFakeInAccessibleNodes(_ context.Context, _ *object.Datastore,
	_ string, _ *vim25.Client) (map[string]bool, error) {
	nodes := map[string]bool{}
	return nodes, nil
}

func getStotagepoolInstance() *unstructured.Unstructured {

	accessibleNodes := []string{"wdc-node-1"}
	compatibleStorageClasses := []string{"sample-ftt0"}
	sp := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cns.vmware.com/v1alpha1",
			"kind":       "StoragePool",
			"metadata": map[string]interface{}{
				"name": fakeSPName,
			},
			"spec": map[string]interface{}{
				"driver": "csi.vsphere.vmware.com",
				"parameters": map[string]interface{}{
					"datastoreUrl": "ds:///vmfs/volumes/vsan:5230620c19c89030-1df1c2b3107af012/",
				},
			},
			"status": map[string]interface{}{
				"accessibleNodes":          accessibleNodes,
				"compatibleStorageClasses": compatibleStorageClasses,
				"capacity": map[string]interface{}{
					"total":     "880392798208",
					"freeSpace": "723378944087",
				},
			},
		},
	}

	return sp
}

func getNodeSpec(testNodeName string, annotations map[string]string) *v1.Node {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testNodeName,
			Annotations: annotations,
		},
	}
	return node
}

func patchNode(testNodeName string, annotations map[string]string, k8sClient *testclient.Clientset) {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}

	patchBytes, _ := json.Marshal(patch)
	ctx := context.Background()
	k8sClient.CoreV1().Nodes().Patch(ctx, testNodeName, k8stypes.MergePatchType, patchBytes, metav1.PatchOptions{})
}

// datastore properties where datastore is in MM
func getFakeDatastorePropertiesDSInMM(ctx context.Context, d *cnsvsphere.DatastoreInfo) *dsProps {
	c := resource.MustParse(capacityStr)
	f := resource.MustParse(freeSpaceStr)
	prop := &dsProps{
		capacity:   &c,
		freeSpace:  &f,
		dsURL:      "/tmp/govcsim-DC0-LocalDS_0",
		dsType:     "cns.vmware.com/OTHER",
		accessible: true,
		inMM:       true,
	}
	return prop
}

// datastore properties where DS is not accessible
func getFakeDatastorePropertiesDSInaccessible(ctx context.Context, d *cnsvsphere.DatastoreInfo) *dsProps {
	c := resource.MustParse(capacityStr)
	f := resource.MustParse(freeSpaceStr)
	prop := &dsProps{
		capacity:   &c,
		freeSpace:  &f,
		dsURL:      "/tmp/govcsim-DC0-LocalDS_0",
		dsType:     "cns.vmware.com/OTHER",
		accessible: false,
		inMM:       false,
	}
	return prop
}
