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
	"flag"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	vmoperatorv1alpha1 "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	apiutils "sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/cnsoperator"
	migrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	internalapis "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis"
	cnsvolumeoperationrequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/csinodetopology/v1alpha1"
)

const (
	timeout      = 60 * time.Second
	pollTime     = 5 * time.Second
	manifestPath = "/config"
)

// GetKubeConfig helps retrieve Kubernetes Config.
func GetKubeConfig(ctx context.Context) (*restclient.Config, error) {
	log := logger.GetLogger(ctx)
	var config *restclient.Config
	var err error
	kubecfgPath := os.Getenv(clientcmd.RecommendedConfigPathEnvVar)
	kubecfgFlag := flag.Lookup("kubeconfig")
	if kubecfgFlag != nil {
		kubecfgPath = kubecfgFlag.Value.(flag.Getter).Get().(string)
	}
	if kubecfgPath != "" {
		log.Infof("k8s client using kubeconfig from %s", kubecfgPath)
		config, err = clientcmd.BuildConfigFromFlags("", kubecfgPath)
		if err != nil {
			log.Errorf("BuildConfigFromFlags failed %v", err)
			return nil, err
		}
	} else {
		log.Info("k8s client using in-cluster config")
		config, err = restclient.InClusterConfig()
		if err != nil {
			log.Errorf("InClusterConfig failed %v", err)
			return nil, err
		}
	}
	config.QPS, config.Burst = getClientThroughput(ctx, false)
	return config, nil
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

// GetRestClientConfigForSupervisor returns restclient config for given
// endpoint, port, certificate and token.
func GetRestClientConfigForSupervisor(ctx context.Context, endpoint string, port string) *restclient.Config {
	log := logger.GetLogger(ctx)
	var config *restclient.Config
	const (
		tokenFile  = cnsconfig.DefaultpvCSIProviderPath + "/token"
		rootCAFile = cnsconfig.DefaultpvCSIProviderPath + "/ca.crt"
	)
	token, err := ioutil.ReadFile(tokenFile)
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

// NewClientForGroup creates a new controller-runtime client for a new scheme.
// The input Group is added to this scheme.
func NewClientForGroup(ctx context.Context, config *restclient.Config, groupName string) (client.Client, error) {
	var err error
	log := logger.GetLogger(ctx)

	scheme := runtime.NewScheme()
	switch groupName {
	case vmoperatorv1alpha1.GroupName:
		err = vmoperatorv1alpha1.AddToScheme(scheme)
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
		err = cnsvolumeoperationrequestv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		err = csinodetopologyv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Errorf("failed to add CSINodeTopology to scheme with error: %+v", err)
			return nil, err
		}
	}
	client, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		log.Errorf("failed to create client for group %s with err: %+v", groupName, err)
	}
	return client, err

}

// NewCnsFileAccessConfigWatcher creates a new ListWatch for VirtualMachines
// given rest client config.
func NewCnsFileAccessConfigWatcher(ctx context.Context, config *restclient.Config, namespace string) (*cache.ListWatch, error) {
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

	client, err := apiutils.RESTClientForGVK(gvk, false, config, serializer.NewCodecFactory(scheme))
	if err != nil {
		log.Errorf("failed to create RESTClient with err: %+v", err)
		return nil, err
	}
	return cache.NewListWatchFromClient(client, cnsfileaccessconfigKind, namespace, fields.Everything()), nil
}

// NewVirtualMachineWatcher creates a new ListWatch for VirtualMachines given
// rest client config.
func NewVirtualMachineWatcher(ctx context.Context, config *restclient.Config, namespace string) (*cache.ListWatch, error) {
	var err error
	log := logger.GetLogger(ctx)

	scheme := runtime.NewScheme()
	err = vmoperatorv1alpha1.AddToScheme(scheme)
	if err != nil {
		log.Errorf("failed to add to scheme with err: %+v", err)
	}
	gvk := schema.GroupVersionKind{
		Group:   vmoperatorv1alpha1.SchemeGroupVersion.Group,
		Version: vmoperatorv1alpha1.SchemeGroupVersion.Version,
		Kind:    virtualMachineKind,
	}

	client, err := apiutils.RESTClientForGVK(gvk, false, config, serializer.NewCodecFactory(scheme))
	if err != nil {
		log.Errorf("failed to create RESTClient with err: %+v", err)
		return nil, err
	}
	return cache.NewListWatchFromClient(client, virtualMachineKind, namespace, fields.Everything()), nil
}

// NewCSINodeTopologyWatcher creates a new ListWatch for CSINodeTopology objects given
// rest client config.
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

	client, err := apiutils.RESTClientForGVK(gvk, false, config, serializer.NewCodecFactory(scheme))
	if err != nil {
		log.Errorf("failed to create RESTClient for %s CR with err: %+v", csiNodeTopologyKind, err)
		return nil, err
	}
	return cache.NewListWatchFromClient(client, csiNodeTopologyKind, v1.NamespaceAll, fields.Everything()), nil
}

// CreateKubernetesClientFromConfig creaates a newk8s client from given kubeConfig file.
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

// GetNodeVMUUID returns vSphere VM UUID set by CCM on the Kubernetes Node.
func GetNodeVMUUID(ctx context.Context, k8sclient clientset.Interface, nodeName string) (string, error) {
	log := logger.GetLogger(ctx)
	log.Infof("GetNodeVMUUID called for the node: %q", nodeName)
	node, err := k8sclient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("failed to get kubernetes node with the name: %q. Err: %v", nodeName, err)
		return "", err
	}
	k8sNodeUUID := cnsvsphere.GetUUIDFromProviderID(node.Spec.ProviderID)
	log.Infof("Retrieved node UUID: %q for the node: %q", k8sNodeUUID, nodeName)
	return k8sNodeUUID, nil
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
		if value, err := strconv.ParseFloat(v, 32); err != nil || float32(value) < minClientQPS || float32(value) > maxClientQPS {
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
	log.Infof("Setting client QPS to %f and Burst to %d.", qps, burst)
	return qps, burst
}

// CreateCustomResourceDefinitionFromSpec creates the custom resource definition
// from given spec. If there is error, function will do the clean up.
func CreateCustomResourceDefinitionFromSpec(ctx context.Context, crdName string, crdSingular string, crdPlural string,
	crdKind string, crdGroup string, crdVersion string, crdScope apiextensionsv1beta1.ResourceScope) error {
	crdSpec := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group: crdGroup,
			Versions: []apiextensionsv1beta1.CustomResourceDefinitionVersion{
				{
					Name:    crdVersion,
					Served:  true,
					Storage: true,
				}},
			Scope: crdScope,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
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
func CreateCustomResourceDefinitionFromManifest(ctx context.Context, fileName string) error {
	log := logger.GetLogger(ctx)
	manifestcrd, err := getCRDFromManifest(ctx, fileName)
	if err != nil {
		log.Errorf("Failed to read the CRD spec from manifest file: %s with err: %+v", fileName, err)
		return err
	}
	return createCustomResourceDefinition(ctx, manifestcrd)

}

// createCustomResourceDefinition takes a custom resource definition spec and
// creates it on the API server.
func createCustomResourceDefinition(ctx context.Context, newCrd *apiextensionsv1beta1.CustomResourceDefinition) error {
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
	crd, err := apiextensionsClientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = apiextensionsClientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Create(ctx, newCrd, metav1.CreateOptions{})
		if err != nil {
			log.Errorf("Failed to create %q CRD with err: %+v", crdName, err)
			return err
		}
		log.Infof("%q CRD created successfully", crdName)
	} else {
		// Update the existing CRD with new CRD.
		crd.Spec = newCrd.Spec
		crd.Status = newCrd.Status
		_, err = apiextensionsClientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Update(ctx, crd, metav1.UpdateOptions{})
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
	err := wait.Poll(pollTime, timeout, func() (bool, error) {
		crd, err := clientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
		if err != nil {
			log.Errorf("Failed to get %q CRD with err: %+v", crdName, err)
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1beta1.Established:
				if cond.Status == apiextensionsv1beta1.ConditionTrue {
					return true, err
				}
			case apiextensionsv1beta1.NamesAccepted:
				if cond.Status == apiextensionsv1beta1.ConditionFalse {
					log.Debugf("Name conflict while waiting for %q CRD creation", cond.Reason)
				}
			}
		}
		return false, err
	})

	// If there is an error, delete the object to keep it clean.
	if err != nil {
		log.Infof("Cleanup %q CRD because the CRD created was not successfully established. Err: %+v", crdName, err)
		deleteErr := clientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(ctx, crdName, *metav1.NewDeleteOptions(0))
		if deleteErr != nil {
			log.Errorf("Failed to delete %q CRD with err: %+v", crdName, deleteErr)
		}
	}
	return err
}

// getCRDFromManifest reads a .json/yaml file and returns the CRD in it.
func getCRDFromManifest(ctx context.Context, fileName string) (*apiextensionsv1beta1.CustomResourceDefinition, error) {
	var crd apiextensionsv1beta1.CustomResourceDefinition
	log := logger.GetLogger(ctx)

	fullPath := filepath.Join(manifestPath, fileName)
	data, err := ioutil.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Errorf("Manifest file: %s doesn't exist. Error: %+v", fullPath, err)
		} else {
			log.Errorf("Failed to read the manifest file: %s. Error: %+v", fullPath, err)
		}
		return nil, err
	}
	json, err := utilyaml.ToJSON(data)
	if err != nil {
		log.Errorf("Failed to convert the manifest file: %s content to JSON with error: %+v", fullPath, err)
		return nil, err
	}

	if err := runtime.DecodeInto(legacyscheme.Codecs.UniversalDecoder(), json, &crd); err != nil {
		log.Errorf("Failed to decode json content: %+v to crd with error: %+v", json, err)
		return nil, err
	}
	return &crd, nil
}
