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

package kubernetes

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	vmoperatorv1alpha1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmoperatorv1alpha2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoperatorv1alpha3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	vmoperatorv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	ccV1beta1 "sigs.k8s.io/cluster-api/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	apiutils "sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	cr_log "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/go-logr/zapr"

	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	storagev1 "k8s.io/api/storage/v1"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	migrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration/v1alpha1"
	storagepoolAPIs "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool"
	wcpcapapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/wcpcapabilities"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
)

const (
	timeout  = 60 * time.Second
	pollTime = 5 * time.Second
)

// GetKubeConfig helps retrieve Kubernetes Config.
func GetKubeConfig(ctx context.Context) (*restclient.Config, error) {
	log := logger.GetLogger(ctx)
	var config *restclient.Config
	var err error
	// TODO-perf: can this be cached?
	kubecfgPath := getKubeConfigPath(ctx)
	if kubecfgPath != "" {
		log.Debugf("k8s client using kubeconfig from %s", kubecfgPath)
		config, err = clientcmd.BuildConfigFromFlags("", kubecfgPath)
		if err != nil {
			log.Errorf("BuildConfigFromFlags failed %v", err)
			return nil, err
		}
	} else {
		log.Debug("k8s client using in-cluster config")
		config, err = restclient.InClusterConfig()
		if err != nil {
			log.Errorf("InClusterConfig failed %v", err)
			return nil, err
		}
	}
	config.QPS, config.Burst = getClientThroughput(ctx, false)
	return config, nil
}

// getKubeConfigPath returns the Kubeconfig path from the environment variable KUBECONFIG.
// If the kubeconfig flag is supplied as an argument to the process, then it overrides the value
// obtained from the KUBECONFIG environment variable and uses the supplied flag value.
func getKubeConfigPath(ctx context.Context) string {
	log := logger.GetLogger(ctx)
	var kubecfgPath string
	// Check if the kubeconfig flag is set
	kubecfgFlag := flag.Lookup("kubeconfig")
	if kubecfgFlag != nil {
		flagValue := kubecfgFlag.Value.String()
		if flagValue != "" {
			kubecfgPath = flagValue
			log.Debugf("Kubeconfig path obtained from kubeconfig flag: %q", kubecfgPath)
		} else {
			log.Debug("Kubeconfig flag is set but empty, checking environment variable value")
		}
	} else {
		log.Debug("Kubeconfig flag not set, checking environment variable value")
	}
	if kubecfgPath == "" {
		// Get the Kubeconfig path from the environment variable
		kubecfgPath = os.Getenv(clientcmd.RecommendedConfigPathEnvVar)
		log.Debugf("Kubeconfig path obtained from environment variable %q: %q",
			clientcmd.RecommendedConfigPathEnvVar, kubecfgPath)
	}
	// Final logging of the Kubeconfig path used
	if kubecfgPath == "" {
		log.Debug("No Kubeconfig path found, either from environment variable or flag")
	} else {
		log.Debugf("Final Kubeconfig path used: %q", kubecfgPath)
	}
	return kubecfgPath
}

// NewClient creates a newk8s client based on a service account.
func NewClient(ctx context.Context) (clientset.Interface, error) {
	log := logger.GetLogger(ctx)
	config, err := GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("Failed to get KubeConfig. err: %v", err)
		return nil, err
	}
	return clientset.NewForConfig(config)
}

// NewSnapshotterClient creates a new external-snapshotter client based on a service account.
func NewSnapshotterClient(ctx context.Context) (snapshotterClientSet.Interface, error) {
	log := logger.GetLogger(ctx)
	config, err := GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("Failed to get KubeConfig. err: %v", err)
		return nil, err
	}
	return snapshotterClientSet.NewForConfig(config)
}

// GetRestClientConfigForSupervisor returns restclient config for given
// endpoint, port, certificate and token.
func GetRestClientConfigForSupervisor(ctx context.Context, endpoint string, port string) *restclient.Config {
	log := logger.GetLogger(ctx)
	var config *restclient.Config
	const (
		tokenFile  = cnsconfig.DefaultpvCSIProviderPath + "/token"
		rootCAFile = cnsconfig.DefaultpvCSIProviderPath + "/ca.crt"
	)
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil
	}
	if _, err := certutil.NewPool(rootCAFile); err != nil {
		log.Errorf("Expected to load root CA config from %s, but got err: %v", rootCAFile, err)
		return nil
	}
	config = &restclient.Config{
		Host: "https://" + net.JoinHostPort(endpoint, port),
		TLSClientConfig: restclient.TLSClientConfig{
			CAFile: rootCAFile,
		},
		BearerToken: string(token),
	}
	config.QPS, config.Burst = getClientThroughput(ctx, true)
	return config
}

// NewSupervisorClient creates a new supervisor client for given restClient config.
func NewSupervisorClient(ctx context.Context, config *restclient.Config) (clientset.Interface, error) {
	log := logger.GetLogger(ctx)
	log.Info("Connecting to supervisor cluster using the certs/token in Guest Cluster config")
	client, err := clientset.NewForConfig(config)
	if err != nil {
		log.Error("failed to connect to the supervisor cluster with err: %+v", err)
		return nil, err
	}

	return client, nil

}

// NewSupervisorSnapshotClient creates a new supervisor client for handling snapshot related objects
func NewSupervisorSnapshotClient(ctx context.Context, config *restclient.Config) (
	snapshotterClientSet.Interface, error) {
	log := logger.GetLogger(ctx)
	log.Info("Connecting to supervisor cluster using the certs/token in Guest Cluster " +
		"config to retrieve the snapshotter client")
	client, err := snapshotterClientSet.NewForConfig(config)
	if err != nil {
		log.Error("failed to connect to the supervisor cluster with err: %+v", err)
		return nil, err
	}
	return client, nil

}

// NewClientForGroup creates a new controller-runtime client for a new scheme.
// The input Group is added to this scheme.
func NewClientForGroup(ctx context.Context, config *restclient.Config, groupName string) (client.Client, error) {
	var err error
	log := logger.GetLogger(ctx)

	// Initialize controller-runtime logger to prevent log.SetLogger warning
	cr_log.SetLogger(zapr.NewLogger(log.Desugar()))

	scheme := runtime.NewScheme()
	switch groupName {
	case ccV1beta1.GroupVersion.Group:
		err = ccV1beta1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme for %s with err: %+v", ccV1beta1.GroupVersion.Group, err)
			return nil, err
		}
	case wcpcapapis.GroupName:
		err = wcpcapapis.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
	case vmoperatorv1alpha5.GroupName:
		log.Info("adding scheme for vm-operator version v1alpha1")
		err = vmoperatorv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		log.Info("adding scheme for vm-operator version v1alpha2")
		err = vmoperatorv1alpha2.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		log.Info("adding scheme for vm-operator version v1alpha3")
		err = vmoperatorv1alpha3.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		log.Info("adding scheme for vm-operator version v1alpha4")
		err = vmoperatorv1alpha4.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		log.Info("adding scheme for vm-operator version v1alpha5")
		err = vmoperatorv1alpha5.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
	case cnsoperatorv1alpha1.GroupName:
		err = cnsoperatorv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		err = migrationv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		err = internalapis.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		err = cnsvolumeoprequestv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		err = csinodetopologyv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add CSINodeTopology to scheme with error: %+v", err)
			return nil, err
		}
		err = cnsvolumeinfov1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add CNSVolumeInfo to scheme with error: %+v", err)
			return nil, err
		}

		err = storagepoolAPIs.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add StoragePool scheme with error :%+v", err)
			return nil, err
		}
	}

	c, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		log.Errorf("failed to create client for group %s with err: %+v", groupName, err)
	}
	return c, err
}

// NewCnsFileAccessConfigWatcher creates a new ListWatch for VirtualMachines
// given rest client config.
func NewCnsFileAccessConfigWatcher(ctx context.Context, config *restclient.Config,
	namespace string) (*cache.ListWatch, error) {
	var err error
	log := logger.GetLogger(ctx)

	scheme := runtime.NewScheme()
	err = cnsoperatorv1alpha1.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
	}
	gvk := schema.GroupVersionKind{
		Group:   cnsoperatorv1alpha1.SchemeGroupVersion.Group,
		Version: cnsoperatorv1alpha1.SchemeGroupVersion.Version,
		Kind:    cnsfileaccessconfigKind,
	}

	httpClient, err := restclient.HTTPClientFor(config)
	if err != nil {
		log.Errorf("failed to create Http.Client with err: %+v", err)
		return nil, err
	}

	restClient, err := apiutils.RESTClientForGVK(gvk, false, config,
		serializer.NewCodecFactory(scheme), httpClient)
	if err != nil {
		log.Errorf("failed to create RESTClient with err: %+v", err)
		return nil, err
	}
	return cache.NewListWatchFromClient(restClient, cnsfileaccessconfigKind, namespace, fields.Everything()), nil
}

// NewVirtualMachineWatcher creates a new ListWatch for VirtualMachines given
// rest client config.
func NewVirtualMachineWatcher(ctx context.Context, config *restclient.Config,
	namespace string) (*cache.ListWatch, error) {
	var err error
	log := logger.GetLogger(ctx)

	scheme := runtime.NewScheme()
	log.Info("adding scheme for vm-operator versions v1alpha1, v1alpha2, v1alpha3, v1alpha4, v1alpha5")
	err = vmoperatorv1alpha5.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
	}
	err = vmoperatorv1alpha4.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
	}
	err = vmoperatorv1alpha3.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
	}
	err = vmoperatorv1alpha2.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
	}
	err = vmoperatorv1alpha1.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
	}

	gvk := schema.GroupVersionKind{
		Group:   vmoperatorv1alpha1.GroupVersion.Group,
		Version: vmoperatorv1alpha1.GroupVersion.Version,
		Kind:    virtualMachineKind,
	}

	httpClient, err := restclient.HTTPClientFor(config)
	if err != nil {
		log.Errorf("failed to create Http.Client with err: %+v", err)
		return nil, err
	}

	restClient, err := apiutils.RESTClientForGVK(gvk, false, config,
		serializer.NewCodecFactory(scheme), httpClient)
	if err != nil {
		log.Errorf("failed to create RESTClient with err: %+v", err)
		return nil, err
	}
	return cache.NewListWatchFromClient(restClient, virtualMachineKind, namespace, fields.Everything()), nil
}

// NewCSINodeTopologyWatcher creates a new ListWatch for CSINodeTopology objects
// given rest client config.
func NewCSINodeTopologyWatcher(ctx context.Context, config *restclient.Config) (*cache.ListWatch, error) {
	var err error
	log := logger.GetLogger(ctx)

	scheme := runtime.NewScheme()
	err = csinodetopologyv1alpha1.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
		return nil, err
	}
	gvk := schema.GroupVersionKind{
		Group:   csinodetopologyv1alpha1.GroupName,
		Version: csinodetopologyv1alpha1.Version,
		Kind:    csiNodeTopologyKind,
	}

	httpClient, err := restclient.HTTPClientFor(config)
	if err != nil {
		log.Errorf("failed to create Http.Client with err: %+v", err)
		return nil, err
	}

	restClient, err := apiutils.RESTClientForGVK(gvk, false, config,
		serializer.NewCodecFactory(scheme), httpClient)
	if err != nil {
		log.Errorf("failed to create RESTClient for %s CR with err: %+v", csiNodeTopologyKind, err)
		return nil, err
	}
	return cache.NewListWatchFromClient(restClient, csiNodeTopologyKind, v1.NamespaceAll, fields.Everything()), nil
}

// CreateKubernetesClientFromConfig creaates a newk8s client from given
// kubeConfig file.
func CreateKubernetesClientFromConfig(kubeConfigPath string) (clientset.Interface, error) {

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, err
	}

	client, err := clientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// GetNodeUUID returns node UUID set by CCM on the
// Kubernetes Node API object if is set to true.
// If not set, returns node UUID from K8s CSINode API
// object.
func GetNodeUUID(ctx context.Context,
	k8sclient clientset.Interface, nodeName string) (string, error) {
	log := logger.GetLogger(ctx)
	log.Infof("GetNodeUUID called for the node: %q", nodeName)
	node, err := k8sclient.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("failed to get K8s CSINode with the name: %q. "+
			"Err: %v", nodeName, err)
		return "", err
	}
	nodeId := GetNodeIdFromCSINode(node)
	if nodeId == "" {
		log.Errorf("CSINode: %q with empty provider ID found. "+
			"Err: %v", nodeName, err)
		return "", err
	}
	log.Infof("Retrieved node UUID: %q for the CSINode: %q", nodeId, nodeName)
	return nodeId, nil
}

// getClientThroughput returns the QPS and Burst for the API server client.
// QPS and Burst default to 50.
// The maximum accepted value for QPS or Burst is set to 1000.
// The minimum accepted value for QPS or Burst is set to 5.
func getClientThroughput(ctx context.Context, isSupervisorClient bool) (float32, int) {
	log := logger.GetLogger(ctx)
	var envClientQPS, envClientBurst string
	qps := defaultClientQPS
	burst := defaultClientBurst

	if isSupervisorClient {
		envClientQPS = types.EnvSupervisorClientQPS
		envClientBurst = types.EnvSupervisorClientBurst
	} else {
		envClientQPS = types.EnvInClusterClientQPS
		envClientBurst = types.EnvInClusterClientBurst
	}

	if v := os.Getenv(envClientQPS); v != "" {
		value, err := strconv.ParseFloat(v, 32)
		if err != nil || float32(value) < minClientQPS || float32(value) > maxClientQPS {
			log.Warnf("Invalid value set for env variable %s: %v. Using default value.", envClientQPS, v)
		} else {
			qps = float32(value)
		}
	}
	if v := os.Getenv(envClientBurst); v != "" {
		if value, err := strconv.Atoi(v); err != nil || value < minClientBurst || value > maxClientBurst {
			log.Warnf("Invalid value set for env variable %s: %v. Using default value.", envClientBurst, v)
		} else {
			burst = value
		}
	}
	log.Debugf("Setting client QPS to %f and Burst to %d.", qps, burst)
	return qps, burst
}

// CreateCustomResourceDefinitionFromSpec creates the custom resource definition
// from given spec. If there is error, function will do the clean up.
func CreateCustomResourceDefinitionFromSpec(ctx context.Context, crdName string, crdSingular string, crdPlural string,
	crdKind string, crdGroup string, crdVersion string, crdScope apiextensionsv1.ResourceScope) error {
	crdSpec := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdName,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: crdGroup,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    crdVersion,
					Served:  true,
					Storage: true,
				}},
			Scope: crdScope,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   crdPlural,
				Singular: crdSingular,
				Kind:     crdKind,
			},
		},
	}
	return createCustomResourceDefinition(ctx, crdSpec)
}

// CreateCustomResourceDefinitionFromManifest creates custom resource definition
// spec from manifest file.
func CreateCustomResourceDefinitionFromManifest(ctx context.Context, embedFiles embed.FS, fileName string) error {
	log := logger.GetLogger(ctx)
	manifestcrd, err := getCRDFromManifest(ctx, embedFiles, fileName)
	if err != nil {
		log.Errorf("Failed to read the CRD spec from manifest file: %s with err: %+v", fileName, err)
		return err
	}
	return createCustomResourceDefinition(ctx, manifestcrd)
}

// GetNodeIdFromCSINode gets the UUID from CSINode object
func GetNodeIdFromCSINode(csiNode *storagev1.CSINode) string {
	drivers := csiNode.Spec.Drivers
	for _, driver := range drivers {
		if driver.Name == types.Name {
			return driver.NodeID
		}
	}
	return ""
}

// createCustomResourceDefinition takes a custom resource definition spec and
// creates it on the API server.
func createCustomResourceDefinition(ctx context.Context, newCrd *apiextensionsv1.CustomResourceDefinition) error {
	log := logger.GetLogger(ctx)
	// Get a config to talk to the apiserver.
	cfg, err := GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("failed to get Kubernetes config. Err: %+v", err)
		return err
	}
	apiextensionsClientSet, err := apiextensionsclientset.NewForConfig(cfg)
	if err != nil {
		log.Errorf("failed to create Kubernetes client using config. Err: %+v", err)
		return err
	}

	crdName := newCrd.ObjectMeta.Name
	crd, err := apiextensionsClientSet.ApiextensionsV1().CustomResourceDefinitions().Get(ctx,
		crdName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = apiextensionsClientSet.ApiextensionsV1().CustomResourceDefinitions().Create(ctx,
			newCrd, metav1.CreateOptions{})
		if err != nil {
			log.Errorf("Failed to create %q CRD with err: %+v", crdName, err)
			return err
		}
		log.Infof("%q CRD created successfully", crdName)
	} else {
		// Update the existing CRD with new CRD.
		crd.Spec = newCrd.Spec
		crd.Status = newCrd.Status
		_, err = apiextensionsClientSet.ApiextensionsV1().CustomResourceDefinitions().Update(ctx,
			crd, metav1.UpdateOptions{})
		if err != nil {
			log.Errorf("Failed to update %q CRD with err: %+v", crdName, err)
			return err
		}
		log.Infof("%q CRD updated successfully", crdName)
		return nil
	}

	err = waitForCustomResourceToBeEstablished(ctx, apiextensionsClientSet, crdName)
	if err != nil {
		log.Errorf("CRD %q created but failed to establish. Err: %+v", crdName, err)
	}
	return err
}

// waitForCustomResourceToBeEstablished waits until the CRD status is Established.
func waitForCustomResourceToBeEstablished(ctx context.Context,
	clientSet apiextensionsclientset.Interface, crdName string) error {
	log := logger.GetLogger(ctx)
	err := wait.PollUntilContextTimeout(ctx, pollTime, timeout, true,
		func(ctx context.Context) (bool, error) {
			crd, err := clientSet.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
			if err != nil {
				log.Errorf("Failed to get %q CRD with err: %+v", crdName, err)
				return false, err
			}
			for _, cond := range crd.Status.Conditions {
				switch cond.Type {
				case apiextensionsv1.Established:
					if cond.Status == apiextensionsv1.ConditionTrue {
						return true, err
					}
				case apiextensionsv1.NamesAccepted:
					if cond.Status == apiextensionsv1.ConditionFalse {
						log.Debugf("Name conflict while waiting for %q CRD creation", cond.Reason)
					}
				}
			}
			return false, err
		})

	// If there is an error, delete the object to keep it clean.
	if err != nil {
		log.Infof("Cleanup %q CRD because the CRD created was not successfully established. Err: %+v", crdName, err)
		deleteErr := clientSet.ApiextensionsV1().CustomResourceDefinitions().Delete(ctx,
			crdName, *metav1.NewDeleteOptions(0))
		if deleteErr != nil {
			log.Errorf("Failed to delete %q CRD with err: %+v", crdName, deleteErr)
		}
	}
	return err
}

// getCRDFromManifest reads a .json/yaml file and returns the CRD in it.
func getCRDFromManifest(ctx context.Context, embedFS embed.FS, fileName string) (
	*apiextensionsv1.CustomResourceDefinition, error) {
	var crd apiextensionsv1.CustomResourceDefinition
	log := logger.GetLogger(ctx)
	data, err := embedFS.ReadFile(fileName)
	if err != nil {
		log.Errorf("Failed to read the manifest file: %s. Error: %+v", fileName, err)
		return nil, err
	}
	json, err := utilyaml.ToJSON(data)
	if err != nil {
		log.Errorf("Failed to convert the manifest file: %s content to JSON with error: %+v", fileName, err)
		return nil, err
	}

	if err := runtime.DecodeInto(legacyscheme.Codecs.UniversalDecoder(), json, &crd); err != nil {
		log.Errorf("Failed to decode json content: %+v to crd with error: %+v", json, err)
		return nil, err
	}
	return &crd, nil
}

// GetLatestCRDVersion retrieves the latest version of a Custom Resource Definition (CRD) by its name.
func GetLatestCRDVersion(ctx context.Context, crdName string) (string, error) {
	log := logger.GetLogger(ctx)
	config, err := GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("Failed to get KubeConfig. err: %s", err)
		return "", err
	}

	c, err := apiextensionsclientset.NewForConfig(config)
	if err != nil {
		log.Errorf("Failed to create API extensions client. err: %s", err)
		return "", err
	}

	crd, err := c.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Failed to get CRD %s. Error: %s", crdName, err)
		return "", err
	}

	for _, version := range crd.Spec.Versions {
		if version.Storage {
			// This is the storage version, which is the latest version.
			return version.Name, nil
		}
	}

	err = fmt.Errorf("no storage version found for CRD %s", crdName)
	log.Error(err)
	return "", err
}

// PatchFinalizers updates only the finalizers of the object without modifying other fields
// or incrementing the resource version.
func PatchFinalizers(ctx context.Context, c client.Client, obj client.Object, finalizers []string) error {
	original := obj.DeepCopyObject().(client.Object)
	obj.SetFinalizers(finalizers)
	patch := client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})
	return c.Patch(ctx, obj, patch)
}

// RetainPersistentVolume updates the PersistentVolume's ReclaimPolicy to Retain.
// This is useful to preserve the PersistentVolume even if the associated PersistentVolumeClaim is deleted.
func RetainPersistentVolume(ctx context.Context, k8sClient clientset.Interface, pvName string) error {
	log := logger.GetLogger(ctx)

	if pvName == "" {
		log.Debugf("PersistentVolume name is empty. Exiting...")
		return nil
	}

	log.Debugf("Retaining PersistentVolume %q", pvName)
	pv, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("PersistentVolume %q not found. Exiting...", pvName)
			return nil
		}

		return logger.LogNewErrorf(log, "Failed to get PersistentVolume %q. Error: %s", pvName, err.Error())
	}

	pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
	_, err = k8sClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	if err != nil {
		return logger.LogNewErrorf(log, "Failed to update PersistentVolume %q to retain policy. Error: %s",
			pvName, err.Error())
	}

	log.Debugf("Successfully retained PersistentVolume %q", pvName)
	return nil
}

// DeletePersistentVolumeClaim deletes the PersistentVolumeClaim with the given name and namespace.
func DeletePersistentVolumeClaim(ctx context.Context, k8sClient clientset.Interface,
	pvcName, pvcNamespace string) error {
	log := logger.GetLogger(ctx)

	if pvcName == "" {
		log.Debugf("PVC name is empty. Exiting...")
		return nil
	}

	log.Debugf("Deleting PersistentVolumeClaim %q in namespace %q", pvcName, pvcNamespace)
	err := k8sClient.CoreV1().PersistentVolumeClaims(pvcNamespace).Delete(ctx, pvcName, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("PersistentVolumeClaim %q in namespace %q not found. Exiting...", pvcName, pvcNamespace)
			return nil
		}

		return logger.LogNewErrorf(log, "Failed to delete PersistentVolumeClaim %q in namespace %q. Error: %s",
			pvcName, pvcNamespace, err.Error())
	}

	log.Debugf("Successfully deleted PersistentVolumeClaim %q in namespace %q", pvcName, pvcNamespace)
	return nil
}

// DeletePersistentVolume deletes the PersistentVolume with the given name.
func DeletePersistentVolume(ctx context.Context, k8sClient clientset.Interface, pvName string) error {
	log := logger.GetLogger(ctx)

	if pvName == "" {
		log.Debugf("PersistentVolume name is empty. Exiting...")
		return nil
	}

	log.Debugf("Deleting PersistentVolume %q", pvName)
	err := k8sClient.CoreV1().PersistentVolumes().Delete(ctx, pvName, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debugf("PersistentVolume %q not found. Exiting...", pvName)
			return nil
		}

		return logger.LogNewErrorf(log, "Failed to delete PersistentVolume %q. Error: %s", pvName, err.Error())
	}

	log.Debugf("Successfully deleted PersistentVolume %q", pvName)
	return nil
}

// UpdateStatus updates the status subresource of the given Kubernetes object.
// If the object is a Custom Resource, make sure that the `subresources` field in the
// CustomResourceDefinition includes `status` to enable status subresource updates.
func UpdateStatus(ctx context.Context, c client.Client, obj client.Object) error {
	log := logger.GetLogger(ctx)
	if err := c.Status().Update(ctx, obj); err != nil {
		log.Errorf("Failed to update status for %s %s/%s: %v", obj.GetObjectKind().GroupVersionKind().Kind,
			obj.GetNamespace(), obj.GetName(), err)
		return err
	}

	log.Infof("Successfully updated status for %s %s/%s", obj.GetObjectKind().GroupVersionKind().Kind,
		obj.GetNamespace(), obj.GetName())
	return nil
}
