/*
Copyright 2018 The Kubernetes Authors.

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

package fcd

import (
	"testing"
)

func TestAPIInvalid(t *testing.T) {
	err := checkAPI("test")
	if err == nil {
		t.Error("Excepted failure. Invalid version string")
	}
}

func TestDoesntMeetMinMajorVersion(t *testing.T) {
	err := checkAPI("5.6.0")
	if err == nil {
		t.Error("Excepted failure. Does not meet minimum major version")
	}
}

func TestDoesntMeetMinMinorVersion(t *testing.T) {
	err := checkAPI("6.1.0")
	if err == nil {
		t.Error("Excepted failure. Does not meet minimum minor version")
	}
}

func TestSupportedVersion(t *testing.T) {
	err := checkAPI("6.5.0")
	if err != nil {
		t.Errorf("This is a supported vCenter version err=%v", err)
	}
}
