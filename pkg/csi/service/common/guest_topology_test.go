/*
Copyright 2020 The Kubernetes Authors.

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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGuestAccessibleTopologyHostIdentity(t *testing.T) {
	const legacyHostname = `[{"topology.kubernetes.io/zone":"az1","kubernetes.io/hostname":"esx.example.com"}]`
	for _, tc := range []struct {
		name, raw, host string
		fail            bool
	}{
		{name: "legacy Supervisor", raw: legacyHostname, host: "host-45"},
		{name: "PodVM v2", raw: `[{"topology.kubernetes.io/zone":"az1"}]`, host: "host-45"},
		{name: "shared", raw: `[{"topology.kubernetes.io/zone":"az1"}]`},
		{name: "incomplete local", raw: legacyHostname, fail: true},
		{name: "no zone", raw: `[{"kubernetes.io/hostname":"esx.example.com"}]`, host: "host-45", fail: true},
		{name: "missing topology", host: "host-45", fail: true},
		{name: "empty topology", raw: `[]`, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pvc := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				AnnVolumeAccessibleTopology: tc.raw, AttributeESXiHostMoID: tc.host,
			}}}
			topology, err := GuestAccessibleTopology(pvc)
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, topology, 1)
			require.Equal(t, "az1", topology[0][v1.LabelTopologyZone])
			require.NotContains(t, topology[0], v1.LabelHostname)
			require.Equal(t, tc.host, topology[0][GuestClusterTopologyLabelHost])
			require.Equal(t, tc.raw, pvc.Annotations[AnnVolumeAccessibleTopology])
		})
	}
}
