/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"

	"github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

// deletePVCInParallel deletes PVC in a given namespace in parallel
func createCustomisedStatefulSets(client clientset.Interface, namespace string,
	isParallelPod bool, replicas int32, IsNodeAffinityToSet bool,
	allowedTopologies []v1.TopologySelectorLabelRequirement, allowedTopologyLen int,
	isMultipleStatefulSet bool, stsCount int) *apps.StatefulSet {
	framework.Logf("Preparing StatefulSet Spec")
	statefulset := GetStatefulSetFromManifest(namespace)

	if IsNodeAffinityToSet {
		nodeSelectorTerms := setTopologyNodeAffinitiesForStatefulSetPVs(allowedTopologies, allowedTopologyLen)
		statefulset.Spec.Template.Spec.Affinity = new(v1.Affinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity = new(v1.NodeAffinity)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution = new(v1.NodeSelector)
		statefulset.Spec.Template.Spec.Affinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = nodeSelectorTerms
	}
	if isParallelPod {
		statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
	}
	statefulset.Spec.Replicas = &replicas

	framework.Logf("Creating statefulset")
	CreateStatefulSet(namespace, statefulset, client)

	framework.Logf("Wait for StatefulSet pods to be in up and running state")
	fss.WaitForStatusReadyReplicas(client, statefulset, replicas)
	gomega.Expect(fss.CheckMount(client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
	ssPodsBeforeScaleDown := fss.GetPodList(client, statefulset)
	gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
		fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
	gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
		"Number of Pods in the statefulset should match with number of replicas")

	return statefulset
}

func setTopologyNodeAffinitiesForStatefulSetPVs(allowedTopologies []v1.TopologySelectorLabelRequirement,
	allowedTopologyLen int) []v1.NodeSelectorTerm {
	var nodeSelectorRequirements []v1.NodeSelectorRequirement
	var nodeSelectorTerms []v1.NodeSelectorTerm
	rackTopology := allowedTopologies[len(allowedTopologies)-1]
	if len(rackTopology.Key) == 32 {
		var nodeSelectorTerm v1.NodeSelectorTerm
		var nodeSelectorRequirement v1.NodeSelectorRequirement
		nodeSelectorRequirement.Key = rackTopology.Key
		nodeSelectorRequirement.Operator = "In"
		for i := 0; i < allowedTopologyLen; i++ {
			nodeSelectorRequirement.Values = append(nodeSelectorRequirement.Values, rackTopology.Values[i])
			nodeSelectorTerm.MatchExpressions = append(nodeSelectorRequirements, nodeSelectorRequirement)
		}
		nodeSelectorTerms = append(nodeSelectorTerms, nodeSelectorTerm)
	}
	return nodeSelectorTerms
}

func setSpecificAllowedTopology(allowedTopologies []v1.TopologySelectorLabelRequirement,
	startIndex int, endIndex int) []v1.TopologySelectorLabelRequirement {
	var allowedTopologiesMap []v1.TopologySelectorLabelRequirement
	specifiedAllowedTopology := v1.TopologySelectorLabelRequirement{
		Key:    allowedTopologies[startIndex].Key,
		Values: allowedTopologies[startIndex].Values[startIndex:endIndex],
	}
	allowedTopologiesMap = append(allowedTopologiesMap, specifiedAllowedTopology)

	return allowedTopologiesMap
}
