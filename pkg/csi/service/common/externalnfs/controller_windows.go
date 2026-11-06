//go:build windows
// +build windows

/*
Copyright 2026 The Kubernetes Authors.

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

package externalnfs

import (
	"context"
	"errors"
)

// errNotSupportedOnWindows is returned by the Windows build of this package. The vSphere CSI
// controller (the only caller, via pkg/csi/service/vanilla/controller.go) always runs on a Linux
// control-plane node, so these functions are unreachable in practice on GOOS=windows — this stub
// exists solely so the driver binary still compiles for the Windows *node* component.
var errNotSupportedOnWindows = errors.New("externalnfs: not supported on windows")

// CreateVolume is a Windows stub — see errNotSupportedOnWindows.
func CreateVolume(_ context.Context, _ map[string]string, _ string) (string, map[string]string, error) {
	return "", nil, errNotSupportedOnWindows
}

// DeleteVolume is a Windows stub — see errNotSupportedOnWindows.
func DeleteVolume(_ context.Context, _ string) error {
	return errNotSupportedOnWindows
}
