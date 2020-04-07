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
	"strconv"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	vmoperatorv1alpha1 "github.com/vmware-tanzu/vm-operator-api/api/v1alpha1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	certutil "k8s.io/client-go/util/cert"
	"sigs.k8s.io/controller-runtime/pkg/client"
	apiutils "sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	migrationv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis"
)

const (
	timeout  = 60 * time.Second
	pollTime = 5 * time.Second
)

// GetKubeConfig helps retrieve Kubernetes Config
func GetKubeConfig(ctx context.Context) (*restclient.Config, error) {
	log := logger.GetLogger(ctx)
	var config *restclient.Config
	var err error
	kubecfgPath := os.Getenv(clientcmd.RecommendedConfigPathEnvVar)
	if flag.Lookup("kubeconfig") != nil {
		kubecfgPath = flag.Lookup("kubeconfig").Value.(flag.Getter).Get().(string)
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
	return config, nil
}

// NewClient creates a newk8s client based on a service account
func NewClient(ctx context.Context) (clientset.Interface, error) {
	log := logger.GetLogger(ctx)
	config, err := GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("Failed to get KubeConfig. err: %v", err)
		return nil, err
	}
	return clientset.NewForConfig(config)
}

// GetRestClientConfig returns restclient config for given endpoint, port, certificate and token
func GetRestClientConfig(ctx context.Context, endpoint string, port string) *restclient.Config {
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

	return config
}

// NewSupervisorClient creates a new supervisor client for given restClient config
func NewSupervisorClient(ctx context.Context, config *restclient.Config) (clientset.Interface, error) {
	log := logger.GetLogger(ctx)
	config.QPS, config.Burst = getSupervisorClientThroughput(ctx)
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
			log.Error("failed to add to scheme with err: %+v", err)
			return nil, err
		}
	case cnsoperatorv1alpha1.GroupName:
		err = cnsoperatorv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Error("failed to add to scheme with err: %+v", err)
			return nil, err
		}
		err = migrationv1alpha1.AddToScheme(scheme)
		if err != nil {
			log.Error("failed to add to scheme with err: %+v", err)
			return nil, err
		}
	}
	client, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		log.Error("failed to create client for group %s with err: %+v", groupName, err)
	}
	return client, err

}

// NewVirtualMachineWatcher creates a new ListWatch for VirtualMachines given rest client config
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

	client, err := apiutils.RESTClientForGVK(gvk, config, serializer.NewCodecFactory(scheme))
	if err != nil {
		log.Error("failed to create RESTClient with err: %+v", err)
		return nil, err
	}
	return cache.NewListWatchFromClient(client, virtualMachineKind, namespace, fields.Everything()), nil
}

// CreateKubernetesClientFromConfig creaates a newk8s client from given kubeConfig file
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

// GetNodeVMUUID returns vSphere VM UUID set by CCM on the Kubernetes Node
func GetNodeVMUUID(ctx context.Context, k8sclient clientset.Interface, nodeName string) (string, error) {
	log := logger.GetLogger(ctx)
	log.Infof("GetNodeVMUUID called for the node: %q", nodeName)
	node, err := k8sclient.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("failed to get kubernetes node with the name: %q. Err: %v", nodeName, err)
		return "", err
	}
	k8sNodeUUID := common.GetUUIDFromProviderID(node.Spec.ProviderID)
	log.Infof("Retrieved node UUID: %q for the node: %q", k8sNodeUUID, nodeName)
	return k8sNodeUUID, nil
}

// getSupervisorClientThroughput returns the QPS and Burst for Supervisor Client provided in env
// variables EnvSupervisorClientQPS and EnvSupervisorClientBurst respectively.
// QPS and Burst default to 50.
// The maximum accepted value for QPS or Burst is set to 1000.
// The minimum accepted value for QPS or Burst is set to 5.
func getSupervisorClientThroughput(ctx context.Context) (float32, int) {
	log := logger.GetLogger(ctx)
	qps := defaultSupervisorClientQPS
	burst := defaultSupervisorClientBurst
	if v := os.Getenv(types.EnvSupervisorClientQPS); v != "" {
		if value, err := strconv.ParseFloat(v, 32); err != nil || float32(value) < minSupervisorClientQPS || float32(value) > maxSupervisorClientQPS {
			log.Warnf("Invalid value set for env variable %s: %v. Using default value.", types.EnvSupervisorClientQPS, v)
		} else {
			qps = float32(value)
		}
	}
	if v := os.Getenv(types.EnvSupervisorClientBurst); v != "" {
		if value, err := strconv.Atoi(v); err != nil || value < minSupervisorClientBurst || value > maxSupervisorClientBurst {
			log.Warnf("Invalid value set for env variable %s: %v. Using default value.", types.EnvSupervisorClientBurst, v)
		} else {
			burst = value
		}
	}
	log.Infof("Setting Supervisor client QPS to %f and Burst to %d.", qps, burst)
	return qps, burst
}

// CreateCustomResourceDefinition creates the CRD and add it into Kubernetes.
// If there is error, function will do the clean up
func CreateCustomResourceDefinition(ctx context.Context, crdName string, crdSingular string, crdPlural string,
	crdKind string, crdGroup string, crdVersion string, crdScope apiextensionsv1beta1.ResourceScope) error {
	log := logger.GetLogger(ctx)
	// Get a config to talk to the apiserver
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
	crd := &apiextensionsv1beta1.CustomResourceDefinition{
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
	_, err = apiextensionsClientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err == nil {
		log.Infof("%q CRD created successfully", crdName)
	} else if apierrors.IsAlreadyExists(err) {
		log.Infof("%q CRD already exists", crdName)
		return nil
	} else {
		log.Errorf("failed to create %q CRD with err: %+v", crdName, err)
		return err
	}

	// CRD takes some time to be established
	// Creating an instance of non-established runs into errors. So, wait for CRD to be created
	err = wait.Poll(pollTime, timeout, func() (bool, error) {
		crd, err = apiextensionsClientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Get(crdName, metav1.GetOptions{})
		if err != nil {
			log.Errorf("failed to get %q CRD with err: %+v", crdName, err)
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
		log.Infof("Cleanup %q CRD because the CRD created was not successfully established. Error: %+v", crdName, err)
		deleteErr := apiextensionsClientSet.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(crdName, nil)
		if deleteErr != nil {
			log.Errorf("failed to delete %q CRD with error: %+v", crdName, deleteErr)
		}
	}
	return err
}

// GetDynamicInformer returns informer for specified CRD group, version and name
// return error if failure observed
func GetDynamicInformer(ctx context.Context, crdGroup string, crdVersion string, crdName string, namespace string) (informers.GenericInformer, error) {
	log := logger.GetLogger(ctx)
	// Get a config to talk to the apiserver
	cfg, err := GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("failed to get Kubernetes config. Err: %+v", err)
		return nil, err
	}
	// Grab a dynamic interface to create informers from
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Errorf("could not generate dynamic client for config. err :%v", err)
		return nil, err
	}
	var informerFactory dynamicinformer.DynamicSharedInformerFactory
	gvr := schema.GroupVersionResource{Group: crdGroup, Version: crdVersion, Resource: crdName}
	if namespace != "" {
		informerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dc, 0, namespace, nil)
	} else {
		informerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dc, 0, metav1.NamespaceAll, nil)
	}
	return informerFactory.ForResource(gvr), nil
}
