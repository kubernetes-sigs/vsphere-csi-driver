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

package wcp

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// MinSupportedVCenterMajor is the minimum, major version of vCenter
	// on which WCP CSI controller is supported.
	MinSupportedVCenterMajor int = 6

	// MinSupportedVCenterMinor is the minimum, minor version of vCenter
	// on which WCP CSI controller is supported.
	MinSupportedVCenterMinor int = 7
)

// checkAPI checks if specified version is 6.7.1 or higher
func checkAPI(version string) error {
	items := strings.Split(version, ".")
	if len(items) <= 1 || len(items) > 3 {
		return fmt.Errorf("Invalid API Version format")
	}
	major, err := strconv.Atoi(items[0])
	if err != nil {
		return fmt.Errorf("Invalid Major Version value")
	}
	minor, err := strconv.Atoi(items[1])
	if err != nil {
		return fmt.Errorf("Invalid Minor Version value")
	}

	if major < MinSupportedVCenterMajor || (major == MinSupportedVCenterMajor && minor < MinSupportedVCenterMinor) {
		return fmt.Errorf("The minimum supported vCenter is 6.7.1")
	}

	if major == MinSupportedVCenterMajor && minor == MinSupportedVCenterMinor && len(items) == 2 {
		patch, err := strconv.Atoi(items[2])
		if err != nil || patch < 1 {
			return fmt.Errorf("Invalid patch version value")
		}
	}
	return nil
}
