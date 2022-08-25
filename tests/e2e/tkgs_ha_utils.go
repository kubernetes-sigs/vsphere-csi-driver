/*
Copyright 2022 The Kubernetes Authors.

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
	"strings"

	v1 "k8s.io/api/core/v1"
)

// checkAnnotationOnSvcPvc checks tkg HA specific annotations on SVC PVC
func checkAnnotationOnSvcPvc(svcPVC *v1.PersistentVolumeClaim,
	allowedTopologies map[string][]string, categories []string) error {
	annotationsMap := svcPVC.Annotations
	if accessibleTopoString, x := annotationsMap[tkgHAccessibleAnnotationKey]; x {
		accessibleTopology := strings.Split(accessibleTopoString, ":")
		topoKey := strings.Split(accessibleTopology[0], "{")[1]
		topoVal := strings.Split(accessibleTopology[1], "}")[0]
		category := strings.SplitAfter(topoKey, "/")[1]
		categoryKey := strings.Split(category, `"`)[0]
		if isValuePresentInTheList(categories, categoryKey) {
			if isValuePresentInTheList(allowedTopologies[topoKey], topoVal) {
				return fmt.Errorf("couldn't find allowed accessible topology: %v on svc pvc: %s"+
					"instead found: %v", allowedTopologies[topoKey], svcPVC.Name, topoVal)
			}
		} else {
			return fmt.Errorf("couldn't find key: %s on allowed categories %v",
				category, categories)
		}
	} else {
		return fmt.Errorf("couldn't find annotation key: %s on svc pvc: %s",
			tkgHAccessibleAnnotationKey, svcPVC.Name)
	}

	if requestedTopoString, y := annotationsMap[tkgHARequestedAnnotationKey]; y {
		availabilityTopo := strings.Split(requestedTopoString, ",")
		for _, avlTopo := range availabilityTopo {
			requestedTopology := strings.Split(avlTopo, ":")
			topoKey := strings.Split(requestedTopology[0], "{")[1]
			topoVal := strings.Split(requestedTopology[1], "}")[0]
			category := strings.SplitAfter(topoKey, "/")[1]
			categoryKey := strings.Split(category, `"`)[0]
			if isValuePresentInTheList(categories, categoryKey) {
				if isValuePresentInTheList(allowedTopologies[topoKey], topoVal) {
					return fmt.Errorf("couldn't find allowed accessible topology: %v on svc pvc: %s"+
						"instead found: %v", allowedTopologies[topoKey], svcPVC.Name, topoVal)
				}
			} else {
				return fmt.Errorf("couldn't find key: %s on allowed categories %v",
					category, categories)
			}
		}
	} else {
		return fmt.Errorf("couldn't find annotation key: %s on svc pvc: %s",
			tkgHARequestedAnnotationKey, svcPVC.Name)
	}
	return nil
}

// isValuePresentInTheList is a util method which checks whether a particular string
// is present in a given list or not
func isValuePresentInTheList(strArr []string, str string) bool {
	for _, s := range strArr {
		if strings.Contains(s, str) {
			return true
		}
	}
	return false
}
