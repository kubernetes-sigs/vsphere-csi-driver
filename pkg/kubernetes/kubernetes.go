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
	"k8s.io/klog"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"net"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClient creates a newk8s client based on a service account
func NewClient() (clientset.Interface, error) {

	var config *restclient.Config
	var err error
	klog.V(2).Info("k8s client using in-cluster config")
	config, err = restclient.InClusterConfig()
	if err != nil {
		klog.Errorf("InClusterConfig failed %q", err)
		return nil, err
	}

	return clientset.NewForConfig(config)
}

// NewSupervisorClient creates a new Guest Cluster client from given endpoint, port, certificate and token
func NewSupervisorClient(endpoint string, port string, certificate string, token string) (clientset.Interface, error) {
	var config *restclient.Config
	klog.V(2).Info("Connecting to supervisor cluster using the certs/token in Guest Cluster config")
	config = &restclient.Config{
		Host: "https://" + net.JoinHostPort(endpoint, port),
		TLSClientConfig: restclient.TLSClientConfig{
			CAData: []byte(certificate),
		},
		BearerToken: string(token),
	}
	client, err := clientset.NewForConfig(config)
	if err != nil {
		klog.Error("Failed to connect to the supervisor cluster with err: %+v", err)
		return nil, err
	}
	return client, nil
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
func GetNodeVMUUID(k8sclient clientset.Interface, nodeName string) (string, error) {
	klog.V(2).Infof("GetNodeVMUUID called for the node: %q", nodeName)
	node, err := k8sclient.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get kubernetes node with the name: %q. Err: %v", nodeName, err)
		return "", err
	}
	k8sNodeUUID := common.GetUUIDFromProviderID(node.Spec.ProviderID)
	klog.V(2).Infof("Retrieved node UUID: %q for the node: %q", k8sNodeUUID, nodeName)
	return k8sNodeUUID, nil
}
