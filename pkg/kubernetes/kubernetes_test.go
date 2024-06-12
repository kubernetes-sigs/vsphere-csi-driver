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
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
