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

package syncer

import (
	"testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func TestNfsGuestVolumeHealthInvalidVolumeHandle(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	// Missing the "guestnfs:" prefix's expected server#share#subdir shape entirely -
	// decode must fail, and the function must report Inaccessible rather than panic
	// or fall through to a network call with garbage input.
	got := nfsGuestVolumeHealth(ctx, "guestnfs:not-a-valid-id", "default", "test-pvc")
	if got != common.VolHealthStatusInaccessible {
		t.Errorf("nfsGuestVolumeHealth with an undecodable volume handle = %q, want %q",
			got, common.VolHealthStatusInaccessible)
	}
}

func TestNfsGuestVolumeHealthUnreachableServer(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()

	// A well-formed handle (decodes fine) pointing at a server with nothing
	// listening on the portmapper port - connection refused, immediate and
	// deterministic on loopback, so this exercises the full decode + CheckMountable
	// call path without depending on a real NFS server being reachable in CI.
	got := nfsGuestVolumeHealth(ctx, "guestnfs:127.0.0.1#store#pvc-test-uid", "default", "test-pvc")
	if got != common.VolHealthStatusInaccessible {
		t.Errorf("nfsGuestVolumeHealth against an unreachable server = %q, want %q",
			got, common.VolHealthStatusInaccessible)
	}
}
