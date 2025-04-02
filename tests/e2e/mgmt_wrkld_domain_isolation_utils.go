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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

/*
This util will create initial namespace get/post api call request
*/
func createInitialNsApiCallUrl() string {
	vcIp := e2eVSphere.Config.Global.VCenterHostname
	initialUrl := "https://" + vcIp + "/api/vcenter/namespaces/instances/"

	return initialUrl
}

/*
This util will verify supervisor pvc annotation, pv affinity rules,
pod node anotation and cns volume metadata
*/
func verifyPvcAnnotationPvAffinityPodAnnotationInSvc(ctx context.Context, client clientset.Interface,
	statefulset *appsv1.StatefulSet, standalonePod *v1.Pod, deployment *appsv1.Deployment, namespace string,
	allowedTopologies []v1.TopologySelectorLabelRequirement) error {

	// Read topology mapping
	allowedTopologiesMap := createAllowedTopologiesMap(allowedTopologies)
	topologyMap := GetAndExpectStringEnvVar(envTopologyMap)
	_, topologyCategories := createTopologyMapLevel5(topologyMap)

	var podList *v1.PodList
	var err error

	// Determine the pod list based on input (StatefulSet, StandalonePod, or Deployment)
	if statefulset != nil {
		// If statefulset is provided, get the pod list associated with it
		podList = GetListOfPodsInSts(client, statefulset)
	} else if standalonePod != nil {
		// If standalonePod is provided, create a PodList with that single pod
		podList = &v1.PodList{Items: []v1.Pod{*standalonePod}}
	} else if deployment != nil {
		// If deployment is provided, get the pod list associated with it
		podList, err = fdep.GetPodsForDeployment(ctx, client, deployment)
		if err != nil {
			return fmt.Errorf("failed to get pods for deployment %s in namespace %s: %w", deployment.Name, namespace, err)
		}
	}

	// Verify annotations and affinity for each pod in the pod list
	for _, pod := range podList.Items {
		// Get Pod details
		_, err := client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pod %s in namespace %s: %w", pod.Name, namespace, err)
		}

		framework.Logf("Verifying PVC annotation and PV affinity rules for pod %s", pod.Name)

		for _, volumespec := range pod.Spec.Volumes {
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
				_, err = verifyPodLocationLevel5(&pod, nodeList, allowedTopologiesMap)
				if err != nil {
					return fmt.Errorf("pod node annotation verification failed for pod %s: %w", pod.Name, err)
				}

				// Verify CNS volume metadata
				err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle, svPvcName, pv.ObjectMeta.Name, pod.Name)
				if err != nil {
					return fmt.Errorf("CNS volume metadata verification failed for pod %s: %w", pod.Name, err)
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

func markZoneForRemovalFromWcpNs(vcRestSessionId string, namespace string, zone string) error {
	statusCode := markZoneForRemovalFromNs(namespace, zone, vcRestSessionId)
	gomega.Expect(statusCode).Should(gomega.BeNumerically("==", 204))
	return checkStatusCode(204, statusCode)
}

func addZoneToWcpNs(vcRestSessionId string, namespace string, zoneName string) error {
	statusCode := addZoneToNs(namespace, zoneName, vcRestSessionId)
	return checkStatusCode(204, statusCode)
}

/*
invokeVCRestAPIPatchRequest invokes PATCH call to edit already
created namespace
*/
func invokeVCRestAPIPatchRequest(vcRestSessionId string, url string, reqBody string) ([]byte, int) {
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: transCfg}
	framework.Logf("Invoking PATCH on url: %s", url)

	req, err := http.NewRequest("PATCH", url, strings.NewReader(reqBody))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req.Header.Add(vcRestSessionIdHeaderName, vcRestSessionId)
	req.Header.Add("Content-Type", "application/json")

	resp, statusCode := httpRequest(httpClient, req)

	return resp, statusCode
}

/*
Add zone to namespace with WaitGroup
*/
func addZoneToWcpNsWithWg(vcRestSessionId string,
	namespace string,
	zoneName string,
	expectedStatusCode []int,
	wg *sync.WaitGroup) {
	defer wg.Done()

	statusCode := addZoneToNs(namespace, zoneName, vcRestSessionId)

	if !isAvailable(expectedStatusCode, statusCode) {
		framework.Logf("failed to add zone %s to NS %s, received status code: %d", zoneName, namespace, statusCode)
	}
}

/*
Add zone to namespace without checking the statuscode
*/
func addZoneToNs(namespace string, zoneName string, vcRestSessionId string) int {
	initailUrl := createInitialNsApiCallUrl()
	AddZoneToNs := initailUrl + namespace

	// Create the request body with zone name inside a zones array
	reqBody := fmt.Sprintf(`{
        "zones": [{"name": "%s"}]
    }`, zoneName)

	// Print the request body for debugging
	fmt.Println(reqBody)

	// Make the API request
	_, statusCode := invokeVCRestAPIPatchRequest(vcRestSessionId, AddZoneToNs, reqBody)
	return statusCode
}

/*
Mark zone for removal with expected success/failure statuscode and WG
*/
func markZoneForRemovalFromWcpNsWithWg(vcRestSessionId string,
	namespace string,
	zone string,
	expectedStatusCode []int,
	wg *sync.WaitGroup) {
	defer wg.Done()
	statusCode := markZoneForRemovalFromNs(namespace, zone, vcRestSessionId)
	if !isAvailable(expectedStatusCode, statusCode) {
		framework.Logf("failed to remove zone %s from namespace %s, received status code: %d", zone, namespace, statusCode)
	}
}

/*
Restart CSI driver with WaitGroup
*/
func restartCSIDriverWithWg(ctx context.Context, client clientset.Interface, namespace string,
	csiReplicas int32, wg *sync.WaitGroup) (bool, error) {
	defer wg.Done()
	return restartCSIDriver(ctx, client, namespace, csiReplicas)
}

/*
Mark zone for removal without checking the statuscode
*/
func markZoneForRemovalFromNs(namespace string, zone string, vcRestSessionId string) int {
	initailUrl := createInitialNsApiCallUrl()
	deleteZoneFromNs := initailUrl + namespace + "/zones/" + zone
	fmt.Println(deleteZoneFromNs)
	_, statusCode := invokeVCRestAPIDeleteRequest(vcRestSessionId, deleteZoneFromNs)
	return statusCode
}

/*
This function creates a wcp namespace in a vSphere supervisor Cluster, associating it
with multiple storage policies and zones.
It constructs an API request and sends it to the vSphere REST API.
*/
func createtWcpNsWithZonesAndPolicies(
	vcRestSessionId string, storagePolicyId []string,
	supervisorId string, zoneNames []string) (string, int, error) {

	r := rand.New(rand.NewSource(time.Now().Unix()))
	namespace := fmt.Sprintf("csi-vmsvcns-%v", r.Intn(10000))
	initailUrl := createInitialNsApiCallUrl()
	nsCreationUrl := initailUrl + "v2"

	var storageSpecs []map[string]string
	for _, policyId := range storagePolicyId {
		storageSpecs = append(storageSpecs, map[string]string{"policy": policyId})
	}

	var zones []map[string]string
	for _, zone := range zoneNames {
		zones = append(zones, map[string]string{"name": zone})
	}

	// Create request body struct
	requestBody := map[string]interface{}{
		"namespace":     namespace,
		"storage_specs": storageSpecs,
		"supervisor":    supervisorId,
		"zones":         zones,
	}

	reqBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return "", 500, fmt.Errorf("error marshalling request body: %w", err)
	}

	reqBody := string(reqBodyBytes)
	fmt.Println(reqBody)

	// Make the API request
	_, statusCode := invokeVCRestAPIPostRequest(vcRestSessionId, nsCreationUrl, reqBody)

	return namespace, statusCode, nil
}

/*
This function generates a PVC specification with requested topology annotation.
It ensures that the PVC is created in a specific zone
*/
func PvcSpecWithRequestedTopology(namespace string, ds string, storageclass *storagev1.StorageClass,
	pvclaimlabels map[string]string, accessMode v1.PersistentVolumeAccessMode, zone string) *v1.PersistentVolumeClaim {
	disksize := diskSize
	topologyAnnotation := fmt.Sprintf(`[{"topology.kubernetes.io/zone":"%s"}]`, zone)
	if ds != "" {
		disksize = ds
	}
	if accessMode == "" {
		// If accessMode is not specified, set the default accessMode.
		accessMode = v1.ReadWriteOnce
	}
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-",
			Namespace:    namespace,
			Annotations: map[string]string{
				"csi.vsphere.volume-requested-topology": topologyAnnotation,
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(disksize),
				},
			},
			StorageClassName: &(storageclass.Name),
		},
	}

	if pvclaimlabels != nil {
		claim.Labels = pvclaimlabels
	}
	return claim
}

/*
This function is responsible for creating a PVC with requested topology annotation.
It ensures that the PVC is bound to a specific storage class and zone, allowing
for proper storage placement in a multi-zone cluster.
*/
func createPvcWithRequestedTopology(ctx context.Context, client clientset.Interface, pvcnamespace string,
	pvclaimlabels map[string]string, ds string, storageclass *storagev1.StorageClass,
	accessMode v1.PersistentVolumeAccessMode, zone string) (*v1.PersistentVolumeClaim, error) {
	pvcspec := PvcSpecWithRequestedTopology(pvcnamespace, ds, storageclass, pvclaimlabels, accessMode, zone)
	ginkgo.By(fmt.Sprintf("Creating PVC in namespace: %s using Storage Class: %s,"+
		"Disk Size: %s, Labels: %+v, AccessMode: %+v, Zone: %s",
		pvcnamespace, storageclass.Name, ds, pvclaimlabels, accessMode, zone))
	pvclaim, err := fpv.CreatePVC(ctx, client, pvcnamespace, pvcspec)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to create PVC: %v", err))
	framework.Logf("PVC %v created successfully in namespace: %v", pvclaim.Name, pvcnamespace)
	return pvclaim, err
}
