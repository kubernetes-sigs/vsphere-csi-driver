/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/onsi/gomega"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
)

// GetAndExpectEnvVar returns the value of an environment variable or fails the regression if it's not set.
func GetAndExpectEnvVar(varName string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := logger.GetLogger(ctx)

	varValue, exists := os.LookupEnv(varName)
	if !exists {
		log.Fatalf("Required environment variable not found: %s", varName)
	}
	return varValue
}

// GetAndExpectStringEnvVar parses a string from env variable.
func GetAndExpectStringEnvVar(varName string) string {
	varValue := os.Getenv(varName)
	gomega.Expect(varValue).NotTo(gomega.BeEmpty(), "ENV "+varName+" is not set")
	return varValue
}

// GetAndExpectIntEnvVar parses an int from env variable.
func GetAndExpectIntEnvVar(varName string) int {
	varValue := GetAndExpectStringEnvVar(varName)
	varIntValue, err := strconv.Atoi(varValue)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error Parsing "+varName)
	return varIntValue
}

// GetBoolEnvVarOrDefault returns the boolean value of an environment variable or return default if it's not set
func GetBoolEnvVarOrDefault(varName string, defaultVal bool) bool {
	varValue, exists := os.LookupEnv(varName)
	if !exists {
		return defaultVal
	}

	varBoolValue, err := strconv.ParseBool(varValue)
	if err != nil {
		ctx := context.Background()
		log := logger.GetLogger(ctx)
		log.Warnf("Invalid boolean value for %s: '%s'. Using default: %v", varName, varValue, defaultVal)
		return defaultVal
	}

	return varBoolValue
}

// GetStringEnvVarOrDefault returns the string value of an environment variable or return default if it's not set
func GetStringEnvVarOrDefault(varName string, defaultVal string) string {
	varValue, exists := os.LookupEnv(varName)
	if !exists || strings.TrimSpace(varValue) == "" {
		return defaultVal
	}
	return varValue
}

/*
GetorIgnoreStringEnvVar, retrieves the value of an environment variable while logging
a warning if the variable is not set.
*/
func GetorIgnoreStringEnvVar(varName string) string {
	varValue, exists := os.LookupEnv(varName)
	if !exists {
		ctx := context.Background()
		log := logger.GetLogger(ctx)
		log.Fatalf("Required environment variable not found: %s", varName)
	}
	return varValue
}

/*
getPortNumAndIP function retrieves the SSHD port number for a given IP address,
considering whether the network is private or public.
*/
func GetPortNumAndIP(testbedConfig *config.TestBedConfig, ip string) (string, string, error) {
	port := "22"

	// Strip port if it's included in IP string
	if strings.Contains(ip, ":") {
		ip = strings.Split(ip, ":")[0]
	}

	// Check if running in private network
	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
	if isPrivateNetwork {
		localhost := GetStringEnvVarOrDefault("LOCAL_HOST_IP", constants.DefaultlocalhostIP)

		if p, exists := testbedConfig.IpPortMap[ip]; exists {
			return localhost, p, nil
		}
		return ip, "", fmt.Errorf("port number is missing for IP: %s", ip)
	}

	return ip, port, nil
}
