/*
Copyright 2025 The Kubernetes Authors.
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

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// getControllerRuntimeDetails return the NodeName and PodName for vSphereCSIControllerPod running
func getControllerRuntimeDetails(client clientset.Interface, nameSpace string) ([]string, []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adminClient, _ := initializeClusterClientsByUserRoles(client)
	pods, _ := adminClient.CoreV1().Pods(nameSpace).List(
		ctx,
		metav1.ListOptions{
			FieldSelector: fields.SelectorFromSet(fields.Set{"status.phase": string(v1.PodRunning)}).String(),
		})
	var nodeNameList []string
	var podNameList []string
	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, vSphereCSIControllerPodNamePrefix) {
			framework.Logf("Found vSphereCSIController pod %s PodStatus %s", pod.Name, pod.Status.Phase)
			nodeNameList = append(nodeNameList, pod.Spec.NodeName)
			podNameList = append(podNameList, pod.Name)
		}
	}
	return nodeNameList, podNameList
}

// waitForControllerDeletion wait for the controller pod to be deleted
func waitForControllerDeletion(ctx context.Context, client clientset.Interface, namespace string) error {
	err := wait.PollUntilContextTimeout(ctx, poll, k8sPodTerminationTimeOutLong, true,
		func(ctx context.Context) (bool, error) {
			_, podNameList := getControllerRuntimeDetails(client, namespace)
			if len(podNameList) == 1 {
				framework.Logf("old vsphere-csi-controller pod  has been successfully deleted")
				return true, nil
			}
			framework.Logf("waiting for old vsphere-csi-controller pod to be deleted.")
			return false, nil
		})

	return err

}

// mapK8sMasterNodeWithIPs returns master node name and IP from K8S testbed
func mapK8sMasterNodeWithIPs(client clientset.Interface, nodeNameIPMap map[string]string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adminClient, _ := initializeClusterClientsByUserRoles(client)
	nodes, err := adminClient.CoreV1().Nodes().List(ctx,
		metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/master"})
	if err != nil {
		return err
	}
	if len(nodes.Items) <= 1 {
		return errors.New("K8S testbed does not have more than one master nodes")
	}
	for _, node := range nodes.Items {
		for _, addr := range node.Status.Addresses {
			if addr.Type == v1.NodeExternalIP && addr.Address != "" && net.ParseIP(addr.Address) != nil {
				framework.Logf("Found master node name %s with external IP %s", node.Name, addr.Address)
				nodeNameIPMap[node.Name] = addr.Address
			} else if addr.Type == v1.NodeInternalIP && addr.Address != "" && net.ParseIP(addr.Address) != nil {
				framework.Logf("Found master node name %s with internal IP %s", node.Name, addr.Address)
				nodeNameIPMap[node.Name] = addr.Address
			}
			if _, ok := nodeNameIPMap[node.Name]; !ok {
				framework.Logf("No IP address is found for master node %s", node.Name)
				return fmt.Errorf("IP address is not found for node %s", node.Name)
			}
		}
	}

	return nil
}
