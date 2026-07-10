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

package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// newDiscoveryTestServer stubs just the storage.k8s.io/v1 discovery endpoint that
// volumeAttributesClassAPIAvailable queries via ServerResourcesForGroupVersion.
func newDiscoveryTestServer(t *testing.T, includeVAC bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/apis/storage.k8s.io/v1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		verbs := []string{"create", "delete", "get", "list", "patch", "update", "watch"}
		list := &metav1.APIResourceList{
			GroupVersion: storagev1.SchemeGroupVersion.String(),
			APIResources: []metav1.APIResource{
				{Name: "storageclasses", Namespaced: false, Kind: "StorageClass", Verbs: verbs},
			},
		}
		if includeVAC {
			list.APIResources = append(list.APIResources, metav1.APIResource{
				Name: "volumeattributesclasses", Namespaced: false, Kind: "VolumeAttributesClass", Verbs: verbs,
			})
		}
		payload, err := json.Marshal(list)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	})
	return httptest.NewServer(mux)
}

func TestVolumeAttributesClassAPIAvailable(t *testing.T) {
	t.Run("VolumeAttributesClassesPresent", func(t *testing.T) {
		srv := newDiscoveryTestServer(t, true)
		t.Cleanup(srv.Close)

		ok, err := volumeAttributesClassAPIAvailable(&rest.Config{Host: srv.URL})
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("VolumeAttributesClassesAbsent", func(t *testing.T) {
		srv := newDiscoveryTestServer(t, false)
		t.Cleanup(srv.Close)

		ok, err := volumeAttributesClassAPIAvailable(&rest.Config{Host: srv.URL})
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("GroupVersionNotFound", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/apis/storage.k8s.io/v1", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		ok, err := volumeAttributesClassAPIAvailable(&rest.Config{Host: srv.URL})
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
