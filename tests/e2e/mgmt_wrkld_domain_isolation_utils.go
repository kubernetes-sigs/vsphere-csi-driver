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
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
)

/*
This util will verify supervisor pvc annotation, pv affinity rules,
pod node anotation and cns volume metadata
*/
func verifyAnnotationsAndNodeAffinityForStatefulsetinSvc(ctx context.Context, client clientset.Interface,
	statefulset *appsv1.StatefulSet, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement) error {
	// Read topology mapping
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
	_, topologyCategories := createTopologyMapLevel5(topologyMap)

	framework.Logf("Reading statefulset pod list for node affinity verification")
	ssPodsBeforeScaleDown := GetListOfPodsInSts(client, statefulset)
	for _, sspod := range ssPodsBeforeScaleDown.Items {
		// Get Pod details
		_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pod %s in namespace %s: %w", sspod.Name, namespace, err)
		}

		framework.Logf("Verifying PVC annotation and PV affinity rules")
		for _, volumespec := range sspod.Spec.Volumes {
			if volumespec.PersistentVolumeClaim != nil {
				svPvcName := volumespec.PersistentVolumeClaim.ClaimName
				pv := getPvFromClaim(client, statefulset.Namespace, svPvcName)

				// Get SVC PVC
				svcPVC, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, svPvcName, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("failed to get SVC PVC %s in namespace %s: %w", svPvcName, namespace, err)
				}

				// Get ready and schedulable nodes
				nodeList, err := fnodes.GetReadySchedulableNodes(ctx, client)
				if err != nil {
					return fmt.Errorf("failed to get ready and schedulable nodes: %w", err)
				}
				if len(nodeList.Items) <= 0 {
					return fmt.Errorf("no ready and schedulable nodes found")
				}

				// Verify SV PVC topology annotations
				err = checkPvcTopologyAnnotationOnSvc(svcPVC, allowedTopologiesMap, topologyCategories)
				if err != nil {
					return fmt.Errorf("topology annotation verification failed for SVC PVC %s: %w", svcPVC.Name, err)
				}

				// Verify SV PV node affinity details
				svcPV := getPvFromClaim(client, namespace, svPvcName)
				_, err = verifyVolumeTopologyForLevel5(svcPV, allowedTopologiesMap)
				if err != nil {
					return fmt.Errorf("topology verification failed for SVC PV %s: %w", svcPV.Name, err)
				}

				// Verify pod node annotation
				_, err = verifyPodLocationLevel5(&sspod, nodeList, allowedTopologiesMap)
				if err != nil {
					return fmt.Errorf("pod node annotation verification failed for pod %s: %w", sspod.Name, err)
				}

				// Verify CNS volume metadata
				err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, svPvcName, pv.ObjectMeta.Name, sspod.Name)
				if err != nil {
					return fmt.Errorf("CNS volume metadata verification failed for pod %s: %w", sspod.Name, err)
				}
			}
		}
	}
	return nil
}

// Function to check annotation on a Supervisor PVC
func checkPvcTopologyAnnotationOnSvc(svcPVC *v1.PersistentVolumeClaim,
	allowedTopologies map[string][]string, categories []string) error {

	annotationsMap := svcPVC.Annotations
	if accessibleTopoString, exists := annotationsMap[tkgHAccessibleAnnotationKey]; exists {
		// Parse the accessible topology string
		var accessibleTopologyList []map[string]string
		err := json.Unmarshal([]byte(accessibleTopoString), &accessibleTopologyList)
		if err != nil {
			return fmt.Errorf("failed to parse accessible topology: %v", err)
		}

		for _, topo := range accessibleTopologyList {
			for topoKey, topoVal := range topo {
				if allowedVals, ok := allowedTopologies[topoKey]; ok {
					// Check if topoVal exists in allowedVals
					found := false
					for _, val := range allowedVals {
						if val == topoVal {
							found = true
							break
						}
					}
					if !found {
						return fmt.Errorf("couldn't find allowed accessible topology: %v on svc pvc: %s, instead found: %v",
							allowedVals, svcPVC.Name, topoVal)
					}
				} else {
					category := strings.SplitN(topoKey, "/", 2)
					if len(category) > 1 && !containsItem(categories, category[1]) {
						return fmt.Errorf("couldn't find key: %s in allowed categories %v", category[1], categories)
					}
				}
			}
		}
	} else {
		return fmt.Errorf("couldn't find annotation key: %s on svc pvc: %s",
			tkgHAccessibleAnnotationKey, svcPVC.Name)
	}
	return nil
}

// Helper function to check if a string exists in a slice
func containsItem(slice []string, item string) bool {
	for _, val := range slice {
		if val == item {
			return true
		}
	}
	return false
}

/*
This util createTestWcpNsWithZones will create a wcp namespace which will be tagged to the zone and
storage policy passed in the util parameters
*/
func createTestWcpNsWithZones(
	vcRestSessionId string, storagePolicyId string,
	supervisorId string, zoneNames []string) string {

	vcIp := e2eVSphere.Config.Global.VCenterHostname
	r := rand.New(rand.NewSource(time.Now().Unix()))

	namespace := fmt.Sprintf("csi-vmsvcns-%v", r.Intn(10000))
	nsCreationUrl := "https://" + vcIp + "/api/vcenter/namespaces/instances/v2"

	// Create a string to represent the zones array
	var zonesString string
	for i, zone := range zoneNames {
		if i > 0 {
			zonesString += ","
		}
		zonesString += fmt.Sprintf(`{"name": "%s"}`, zone)
	}

	reqBody := fmt.Sprintf(`{
        "namespace": "%s",
        "storage_specs": [ 
            {
                "policy": "%s"
            }
        ],
        "supervisor": "%s",
        "zones": [%s]
    }`, namespace, storagePolicyId, supervisorId, zonesString)

	// Print the request body for debugging
	fmt.Println(reqBody)

	// Make the API request
	_, statusCode := invokeVCRestAPIPostRequest(vcRestSessionId, nsCreationUrl, reqBody)

	// Validate the status code
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 204))
	framework.Logf("Successfully created namespace %v in SVC.", namespace)
	return namespace
}
