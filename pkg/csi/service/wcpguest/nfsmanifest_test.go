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

package wcpguest

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

const (
	guestManifestsGlob  = "../../../../manifests/guestcluster/*/pvcsi.yaml"
	controllerContainer = "vsphere-csi-controller"
)

var templateAction = regexp.MustCompile(`\{\{[^}]*\}\}`)

// controllerDeployment returns the vsphere-csi-controller Deployment from a guest pvcsi.yaml,
// which is a Go template: the actions are replaced by a placeholder so it parses as YAML.
func controllerDeployment(t *testing.T, manifest string) *appsv1.Deployment {
	t.Helper()
	raw, err := os.ReadFile(manifest)
	require.NoError(t, err)
	rendered := templateAction.ReplaceAllString(string(raw), "placeholder")
	for _, doc := range strings.Split(rendered, "\n---") {
		var meta struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if yaml.Unmarshal([]byte(doc), &meta) != nil || meta.Kind != "Deployment" ||
			meta.Metadata.Name != controllerContainer {
			continue
		}
		var dep appsv1.Deployment
		require.NoError(t, yaml.Unmarshal([]byte(doc), &dep), manifest)
		return &dep
	}
	t.Fatalf("no %s Deployment in %s", controllerContainer, manifest)
	return nil
}

func container(dep *appsv1.Deployment, name string) *corev1.Container {
	for i := range dep.Spec.Template.Spec.Containers {
		if dep.Spec.Template.Spec.Containers[i].Name == name {
			return &dep.Spec.Template.Spec.Containers[i]
		}
	}
	return nil
}

func guestNFSManifests(t *testing.T) []string {
	t.Helper()
	all, err := filepath.Glob(guestManifestsGlob)
	require.NoError(t, err)
	var withNFS []string
	for _, m := range all {
		raw, err := os.ReadFile(m)
		require.NoError(t, err)
		if strings.Contains(string(raw), common.GuestNFSVolume) {
			withNFS = append(withNFS, m)
		}
	}
	require.NotEmpty(t, withNFS, "no guest manifest defines the %s feature state", common.GuestNFSVolume)
	return withNFS
}

// CreateVolume and DeleteVolume mount the NFS export inside the controller container, which needs
// CAP_SYS_ADMIN and, for the capability to take effect, a root user. A manifest that defines the
// guest NFS feature state but leaves the controller unprivileged would make every NFS
// CreateVolume fail with a mount permission error.
func TestGuestManifestControllerCanMountNFS(t *testing.T) {
	for _, manifest := range guestNFSManifests(t) {
		t.Run(manifest, func(t *testing.T) {
			c := container(controllerDeployment(t, manifest), controllerContainer)
			require.NotNil(t, c)
			sc := c.SecurityContext
			require.NotNil(t, sc, "the controller needs an explicit securityContext")
			require.NotNil(t, sc.RunAsNonRoot)
			require.False(t, *sc.RunAsNonRoot, "a capability does nothing for a non-root process")
			require.NotNil(t, sc.RunAsUser)
			require.Zero(t, *sc.RunAsUser)
			require.NotNil(t, sc.Capabilities)
			require.Contains(t, sc.Capabilities.Add, corev1.Capability("SYS_ADMIN"))
		})
	}
}

// Only the controller container mounts anything, so no other container in the Deployment, the
// syncer included, may be given root or SYS_ADMIN along with it.
func TestGuestManifestOnlyControllerContainerGetsMountPermission(t *testing.T) {
	for _, manifest := range guestNFSManifests(t) {
		t.Run(manifest, func(t *testing.T) {
			dep := controllerDeployment(t, manifest)
			require.NotEmpty(t, dep.Spec.Template.Spec.Containers)
			for _, c := range dep.Spec.Template.Spec.Containers {
				if c.Name == controllerContainer {
					continue
				}
				if sc := c.SecurityContext; sc != nil {
					require.True(t, sc.RunAsUser == nil || *sc.RunAsUser != 0, "%s must not run as root", c.Name)
					require.True(t, sc.Privileged == nil || !*sc.Privileged, "%s must not be privileged", c.Name)
					if sc.Capabilities != nil {
						require.NotContains(t, sc.Capabilities.Add, corev1.Capability("SYS_ADMIN"),
							"%s must not get SYS_ADMIN", c.Name)
					}
				}
			}
		})
	}
}
