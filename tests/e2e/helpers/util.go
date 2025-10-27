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
package helpers

import (
	"strconv"
	"strings"

	"k8s.io/kubernetes/test/e2e/framework"
)

// Helper function to check if a string exists in a slice
func ContainsItem(slice []string, item string) bool {
	for _, val := range slice {
		if val == item {
			return true
		}
	}
	return false
}

// isValuePresentInTheList is a util method which checks whether a particular string
// is present in a given list or not
func IsValuePresentInTheList(strArr []string, str string) bool {
	for _, s := range strArr {
		if strings.Contains(s, str) {
			return true
		}
	}
	return false
}

// This method is used to remove the unit and convert string to int
func ParseMi(value string) (int, error) {
	var numeric string
	// Remove the "Mi" or "Gi" suffix and convert to int
	if strings.HasSuffix(value, "Mi") {
		numeric = strings.TrimSuffix(value, "Mi")
	} else if strings.HasSuffix(value, "Gi") {
		numeric = strings.TrimSuffix(value, "Gi")
	}
	framework.Logf("Converting %s to int", numeric)
	return strconv.Atoi(numeric)
}
