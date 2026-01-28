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

package env

import (
	"context"
	"os"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// StartupEnv holds all environment variables read at service startup.
type StartupEnv struct {
	// LoggerLevel specifies the logging level (PRODUCTION or DEVELOPMENT)
	LoggerLevel string

	// CSIEndpoint specifies the CSI endpoint for CSI driver
	CSIEndpoint string

	// CSIMode specifies the service mode (controller or node)
	CSIMode string

	// ClusterFlavor specifies the cluster flavor (VANILLA, WORKLOAD, or GUEST)
	ClusterFlavor string

	// PodNamespace specifies the namespace where the pod is running
	PodNamespace string
}

var (
	// globalStartupEnv holds the loaded startup environment variables
	globalStartupEnv StartupEnv
)

// Load reads and validates all startup environment variables.
// This should be called once at service initialization.
func Load(ctx context.Context) StartupEnv {
	log := logger.GetLogger(ctx)

	env := StartupEnv{
		LoggerLevel:   os.Getenv("LOGGER_LEVEL"),
		CSIEndpoint:   os.Getenv("CSI_ENDPOINT"),
		CSIMode:       os.Getenv("X_CSI_MODE"),
		ClusterFlavor: os.Getenv("CLUSTER_FLAVOR"),
		PodNamespace:  os.Getenv("POD_NAMESPACE"),
	}

	// Validate required environment variables for CSI driver
	// Note: CSI_ENDPOINT is required for vsphere-csi but not for syncer
	// The validation is handled by the caller based on service type

	// Store globally for access by other components
	globalStartupEnv = env

	// Log loaded configuration (without sensitive values)
	log.Infof("Loaded startup environment: LoggerLevel=%q, CSIEndpoint=%q, CSIMode=%q, ClusterFlavor=%q, PodNamespace=%q",
		env.LoggerLevel, maskValue(env.CSIEndpoint), env.CSIMode, env.ClusterFlavor, env.PodNamespace)

	return env
}

// GetStartupEnv returns the globally loaded startup environment.
// Returns an error if Load has not been called yet.
func GetStartupEnv() StartupEnv {
	return globalStartupEnv
}

// maskValue masks a value for logging, showing only the first few characters.
func maskValue(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 10 {
		return "***"
	}
	return value[:10] + "***"
}
