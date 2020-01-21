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
	"io/ioutil"
	"net"

	vmoperatorv1alpha1 "gitlab.eng.vmware.com/core-build/vm-operator-client/pkg/client/clientset/versioned/typed/vmoperator/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	certutil "k8s.io/client-go/util/cert"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	cnsoperatorclient "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/client/clientset/versioned/typed/cns/v1alpha1"
)

// NewClient creates a newk8s client based on a service account
func NewClient(ctx context.Context) (clientset.Interface, error) {
	log := logger.GetLogger(ctx)
	var config *restclient.Config
	var err error
	log.Info("k8s client using in-cluster config")
	config, err = restclient.InClusterConfig()
	if err != nil {
		log.Errorf("InClusterConfig failed %q", err)
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
	log.Info("Connecting to supervisor cluster using the certs/token in Guest Cluster config")
	client, err := clientset.NewForConfig(config)
	if err != nil {
		log.Error("Failed to connect to the supervisor cluster with err: %+v", err)
		return nil, err
	}
	return client, nil

}

// NewCnsVolumeMetadataClient creates a new CnsVolumeMetadata client from the given rest client config
func NewCnsVolumeMetadataClient(ctx context.Context, config *restclient.Config) (*cnsoperatorclient.CnsV1alpha1Client, error) {
	log := logger.GetLogger(ctx)
	client, err := cnsoperatorclient.NewForConfig(config)
	if err != nil {
		log.Error("Failed to connect to the supervisor cluster with err: %+v", err)
		return nil, err
	}
	return client, nil
}

// NewVMOperatorClient creates a new VMOperatorClient for given restClient config
func NewVMOperatorClient(ctx context.Context, config *restclient.Config) (*vmoperatorv1alpha1.VmoperatorV1alpha1Client, error) {
	log := logger.GetLogger(ctx)
	vmOperatorClient, err := vmoperatorv1alpha1.NewForConfig(config)
	if err != nil {
		log.Error("Failed to connect to the supervisor cluster with err: %+v", err)
		return nil, err
	}
	return vmOperatorClient, nil
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
		log.Errorf("Failed to get kubernetes node with the name: %q. Err: %v", nodeName, err)
		return "", err
	}
	k8sNodeUUID := common.GetUUIDFromProviderID(node.Spec.ProviderID)
	log.Infof("Retrieved node UUID: %q for the node: %q", k8sNodeUUID, nodeName)
	return k8sNodeUUID, nil
}
