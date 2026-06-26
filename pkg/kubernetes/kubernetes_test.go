/*
Copyright 2024 The Kubernetes Authors.

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

package kubernetes

import (
	"context"
	"errors"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/clientcmd"
)

func setupFlags() {
	flag.String("kubeconfig", "", "Path to the kubeconfig file")
}

func TestGetKubeConfigPath(t *testing.T) {
	// Helper function to reset flags after each test
	resetFlags := func() {
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		setupFlags()
	}

	// Test case: Environment variable set, flag not set
	t.Run("EnvVarSetFlagNotSet", func(t *testing.T) {
		resetFlags()
		ctx := context.Background()
		expectedPath := "/path/from/env"
		os.Setenv(clientcmd.RecommendedConfigPathEnvVar, expectedPath)
		defer os.Unsetenv(clientcmd.RecommendedConfigPathEnvVar)

		result := getKubeConfigPath(ctx)
		assert.Equal(t, expectedPath, result)
	})

	// Test case: Environment variable not set, flag set
	t.Run("EnvVarNotSetFlagSet", func(t *testing.T) {
		resetFlags()
		ctx := context.Background()
		expectedPath := "/path/from/flag"
		err := flag.Set("kubeconfig", expectedPath)
		assert.Nil(t, err, nil)
		result := getKubeConfigPath(ctx)
		assert.Equal(t, expectedPath, result)
	})

	// Test case: Both environment variable and flag set, flag should take precedence
	t.Run("EnvVarSetFlagSet", func(t *testing.T) {
		resetFlags()
		ctx := context.Background()
		envPath := "/path/from/env"
		flagPath := "/path/from/flag"
		err := os.Setenv(clientcmd.RecommendedConfigPathEnvVar, envPath)
		assert.Nil(t, err, nil)
		defer os.Unsetenv(clientcmd.RecommendedConfigPathEnvVar)
		err = flag.Set("kubeconfig", flagPath)
		assert.Nil(t, err, nil)

		result := getKubeConfigPath(ctx)
		assert.Equal(t, flagPath, result)
	})

	// Test case: Neither environment variable nor flag set
	t.Run("NeitherEnvVarNorFlagSet", func(t *testing.T) {
		resetFlags()
		ctx := context.Background()

		result := getKubeConfigPath(ctx)
		assert.Equal(t, "", result)
	})

	// Test case: Environment variable set, flag set but empty
	t.Run("EnvVarSetFlagEmpty", func(t *testing.T) {
		resetFlags()
		ctx := context.Background()
		expectedPath := "/path/from/env"
		os.Setenv(clientcmd.RecommendedConfigPathEnvVar, expectedPath)
		defer os.Unsetenv(clientcmd.RecommendedConfigPathEnvVar)
		err := flag.Set("kubeconfig", "")
		assert.Nil(t, err, nil)

		result := getKubeConfigPath(ctx)
		assert.Equal(t, expectedPath, result)
	})
}

const (
	testGroup    = "vmware.infrastructure.cluster.x-k8s.io"
	testResource = "vsphereclusters"
)

// mockDiscovery allows tests to control exactly what ServerGroupsAndResources returns.
type mockDiscovery struct {
	discovery.DiscoveryInterface
	groups []*metav1.APIGroup
	lists  []*metav1.APIResourceList
	err    error
}

func (m *mockDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return m.groups, m.lists, m.err
}

func apiGroup(name, preferredVersion string) *metav1.APIGroup {
	return &metav1.APIGroup{
		Name: name,
		PreferredVersion: metav1.GroupVersionForDiscovery{
			Version: preferredVersion,
		},
	}
}

func apiResourceList(groupVersion string, resources ...string) *metav1.APIResourceList {
	list := &metav1.APIResourceList{GroupVersion: groupVersion}
	for _, r := range resources {
		list.APIResources = append(list.APIResources, metav1.APIResource{Name: r})
	}
	return list
}

func TestGetPreferredVersionFromDiscovery_PreferredVersionInGroups(t *testing.T) {
	dc := &mockDiscovery{
		groups: []*metav1.APIGroup{
			apiGroup(testGroup, "v1beta2"),
		},
		lists: []*metav1.APIResourceList{
			apiResourceList(testGroup+"/v1beta2", testResource),
		},
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "v1beta2" {
		t.Errorf("expected v1beta2, got %q", got)
	}
}

func TestGetPreferredVersionFromDiscovery_FallbackToResourceList(t *testing.T) {
	// Group has no PreferredVersion set — should fall through to resource list scan.
	dc := &mockDiscovery{
		groups: []*metav1.APIGroup{
			{Name: testGroup, PreferredVersion: metav1.GroupVersionForDiscovery{Version: ""}},
		},
		lists: []*metav1.APIResourceList{
			apiResourceList(testGroup+"/v1beta3", testResource),
		},
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "v1beta3" {
		t.Errorf("expected v1beta3, got %q", got)
	}
}

func TestGetPreferredVersionFromDiscovery_GroupNotInGroupsList_FoundInResourceList(t *testing.T) {
	// Target group absent from groups slice entirely — fallback list scan must find it.
	dc := &mockDiscovery{
		groups: []*metav1.APIGroup{
			apiGroup("some.other.group", "v1"),
		},
		lists: []*metav1.APIResourceList{
			apiResourceList(testGroup+"/v1alpha1", testResource),
		},
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "v1alpha1" {
		t.Errorf("expected v1alpha1, got %q", got)
	}
}

func TestGetPreferredVersionFromDiscovery_TotalFailure_NilLists(t *testing.T) {
	dc := &mockDiscovery{
		groups: nil,
		lists:  nil,
		err:    errors.New("api server unreachable"),
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got != "" {
		t.Errorf("expected empty string on error, got %q", got)
	}
}

func TestGetPreferredVersionFromDiscovery_PartialFailure_TargetGroupUnavailable(t *testing.T) {
	// ErrGroupDiscoveryFailed with the target group in the failed set.
	// lists is non-nil but does not contain the target group.
	partialErr := &discovery.ErrGroupDiscoveryFailed{
		Groups: map[schema.GroupVersion]error{
			{Group: testGroup, Version: "v1beta2"}: errors.New("timeout"),
		},
	}
	dc := &mockDiscovery{
		groups: []*metav1.APIGroup{
			apiGroup("some.other.group", "v1"),
		},
		lists: []*metav1.APIResourceList{
			apiResourceList("some.other.group/v1", "otherobjects"),
		},
		err: partialErr,
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err == nil {
		t.Fatal("expected error when target group is missing from partial discovery, got nil")
	}
	if got != "" {
		t.Errorf("expected empty string on error, got %q", got)
	}
}

func TestGetPreferredVersionFromDiscovery_PartialFailure_TargetGroupStillAvailable(t *testing.T) {
	// ErrGroupDiscoveryFailed for an unrelated group — target group is still in lists.
	partialErr := &discovery.ErrGroupDiscoveryFailed{
		Groups: map[schema.GroupVersion]error{
			{Group: "unrelated.group", Version: "v1"}: errors.New("timeout"),
		},
	}
	dc := &mockDiscovery{
		groups: []*metav1.APIGroup{
			apiGroup(testGroup, "v1beta2"),
		},
		lists: []*metav1.APIResourceList{
			apiResourceList(testGroup+"/v1beta2", testResource),
		},
		err: partialErr,
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "v1beta2" {
		t.Errorf("expected v1beta2 when target group is available despite partial error, got %q", got)
	}
}

func TestGetPreferredVersionFromDiscovery_GroupAndResourceNotFound(t *testing.T) {
	dc := &mockDiscovery{
		groups: []*metav1.APIGroup{
			apiGroup("some.other.group", "v1"),
		},
		lists: []*metav1.APIResourceList{
			apiResourceList("some.other.group/v1", "otherobjects"),
		},
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err == nil {
		t.Fatal("expected error when group not found, got nil")
	}
	if got != "" {
		t.Errorf("expected empty string on error, got %q", got)
	}
}

func TestGetPreferredVersionFromDiscovery_ResourceNotInMatchingGroup(t *testing.T) {
	// Group is found but does not serve the requested resource.
	dc := &mockDiscovery{
		groups: []*metav1.APIGroup{
			{Name: testGroup, PreferredVersion: metav1.GroupVersionForDiscovery{Version: ""}},
		},
		lists: []*metav1.APIResourceList{
			apiResourceList(testGroup+"/v1beta2", "otherobjects"),
		},
	}

	got, err := getPreferredVersionFromDiscovery(context.Background(), dc, testGroup, testResource)
	if err == nil {
		t.Fatal("expected error when resource not in group, got nil")
	}
	if got != "" {
		t.Errorf("expected empty string on error, got %q", got)
	}
}
