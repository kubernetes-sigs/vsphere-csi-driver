/*
Copyright 2019 The Kubernetes Authors.

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

package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"gopkg.in/gcfg.v1"
	"k8s.io/klog"
)

const (
	// DefaultK8sServiceAccount is the default name of the Kubernetes
	// service account for csi controller.
	DefaultK8sServiceAccount string = "vsphere-csi-controller"
	// DefaultVCenterPort is the default port used to access vCenter.
	DefaultVCenterPort string = "443"
	// DefaultCloudConfigPath is the default path of csi config file
	DefaultCloudConfigPath = "/etc/cloud/csi-vsphere.conf"
	// EnvCloudConfig contains the path to the CSI vSphere Config
	EnvCloudConfig = "VSPHERE_CSI_CONFIG"
)

// Errors
var (
	// ErrUsernameMissing is returned when the provided username is empty.
	ErrUsernameMissing = errors.New("Username is missing")

	// ErrPasswordMissing is returned when the provided password is empty.
	ErrPasswordMissing = errors.New("Password is missing")

	// ErrInvalidVCenterIP is returned when the provided vCenter IP address is
	// missing from the provided configuration.
	ErrInvalidVCenterIP = errors.New("vsphere.conf does not have the VirtualCenter IP address specified")

	// ErrMissingVCenter is returned when the provided configuration does not
	// define any vCenters.
	ErrMissingVCenter = errors.New("No Virtual Center hosts defined")
)

func getEnvKeyValue(match string, partial bool) (string, string, error) {
	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")
		if len(pair) != 2 {
			continue
		}

		key := pair[0]
		value := pair[1]

		if partial && strings.Contains(key, match) {
			return key, value, nil
		}

		if strings.Compare(key, match) == 0 {
			return key, value, nil
		}
	}

	matchType := "match"
	if partial {
		matchType = "partial match"
	}

	return "", "", fmt.Errorf("Failed to find %s with %s", matchType, match)
}

// FromEnv initializes the provided configuratoin object with values
// obtained from environment variables. If an environment variable is set
// for a property that's already initialized, the environment variable's value
// takes precedence.
func FromEnv(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("Config object cannot be nil")
	}
	//Init
	if cfg.VirtualCenter == nil {
		cfg.VirtualCenter = make(map[string]*VirtualCenterConfig)
	}

	//Globals
	if v := os.Getenv("VSPHERE_VCENTER"); v != "" {
		cfg.Global.VCenterIP = v
	}
	if v := os.Getenv("VSPHERE_VCENTER_PORT"); v != "" {
		cfg.Global.VCenterPort = v
	}
	if v := os.Getenv("VSPHERE_USER"); v != "" {
		cfg.Global.User = v
	}
	if v := os.Getenv("VSPHERE_PASSWORD"); v != "" {
		cfg.Global.Password = v
	}
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		cfg.Global.Datacenters = v
	}
	if v := os.Getenv("VSPHERE_INSECURE"); v != "" {
		InsecureFlag, err := strconv.ParseBool(v)
		if err != nil {
			klog.Errorf("Failed to parse VSPHERE_INSECURE: %s", err)
		} else {
			cfg.Global.InsecureFlag = InsecureFlag
		}
	}
	if v := os.Getenv("VSPHERE_LABEL_REGION"); v != "" {
		cfg.Labels.Region = v
	}
	if v := os.Getenv("VSPHERE_LABEL_ZONE"); v != "" {
		cfg.Labels.Zone = v
	}
	//Build VirtualCenter from ENVs
	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")

		if len(pair) != 2 {
			continue
		}

		key := pair[0]
		value := pair[1]

		if strings.HasPrefix(key, "VSPHERE_VCENTER_") && len(value) > 0 {
			id := strings.TrimPrefix(key, "VSPHERE_VCENTER_")
			vcenter := value

			_, username, errUsername := getEnvKeyValue("VCENTER_"+id+"_USERNAME", false)
			if errUsername != nil {
				username = cfg.Global.User
			}
			_, password, errPassword := getEnvKeyValue("VCENTER_"+id+"_PASSWORD", false)
			if errPassword != nil {
				password = cfg.Global.Password
			}
			_, port, errPort := getEnvKeyValue("VCENTER_"+id+"_PORT", false)
			if errPort != nil {
				port = cfg.Global.VCenterPort
			}
			insecureFlag := false
			_, insecureTmp, errInsecure := getEnvKeyValue("VCENTER_"+id+"_INSECURE", false)
			if errInsecure != nil {
				insecureFlagTmp, errTmp := strconv.ParseBool(insecureTmp)
				if errTmp == nil {
					insecureFlag = insecureFlagTmp
				}
			}
			_, datacenters, errDatacenters := getEnvKeyValue("VCENTER_"+id+"_DATACENTERS", false)
			if errDatacenters != nil {
				datacenters = cfg.Global.Datacenters
			}
			cfg.VirtualCenter[vcenter] = &VirtualCenterConfig{
				User:         username,
				Password:     password,
				VCenterPort:  port,
				InsecureFlag: insecureFlag,
				Datacenters:  datacenters,
			}
		}
	}
	if cfg.Global.VCenterIP != "" && cfg.VirtualCenter[cfg.Global.VCenterIP] == nil {
		cfg.VirtualCenter[cfg.Global.VCenterIP] = &VirtualCenterConfig{
			User:         cfg.Global.User,
			Password:     cfg.Global.Password,
			VCenterPort:  cfg.Global.VCenterPort,
			InsecureFlag: cfg.Global.InsecureFlag,
			Datacenters:  cfg.Global.Datacenters,
		}
	}
	err := validateConfig(cfg)
	if err != nil {
		return err
	}

	return nil
}

func validateConfig(cfg *Config) error {
	//Fix default global values
	if cfg.Global.VCenterPort == "" {
		cfg.Global.VCenterPort = DefaultVCenterPort
	}
	// Must have at least one vCenter defined
	if len(cfg.VirtualCenter) == 0 {
		klog.Error(ErrMissingVCenter)
		return ErrMissingVCenter
	}
	for vcServer, vcConfig := range cfg.VirtualCenter {
		klog.V(4).Infof("Initializing vc server %s", vcServer)
		if vcServer == "" {
			klog.Error(ErrInvalidVCenterIP)
			return ErrInvalidVCenterIP
		}

		if vcConfig.User == "" {
			vcConfig.User = cfg.Global.User
			if vcConfig.User == "" {
				klog.Errorf("vcConfig.User is empty for vc %s!", vcServer)
				return ErrUsernameMissing
			}
		}
		if vcConfig.Password == "" {
			vcConfig.Password = cfg.Global.Password
			if vcConfig.Password == "" {
				klog.Errorf("vcConfig.Password is empty for vc %s!", vcServer)
				return ErrPasswordMissing
			}
		}
		if vcConfig.VCenterPort == "" {
			vcConfig.VCenterPort = cfg.Global.VCenterPort
		}
		if vcConfig.Datacenters == "" {
			if cfg.Global.Datacenters != "" {
				vcConfig.Datacenters = cfg.Global.Datacenters
			}
		}
		insecure := vcConfig.InsecureFlag
		if !insecure {
			vcConfig.InsecureFlag = cfg.Global.InsecureFlag
		}
	}
	return nil
}

// ReadConfig parses vSphere cloud config file and stores it into VSphereConfig.
// Environment variables are also checked
func ReadConfig(config io.Reader) (*Config, error) {
	if config == nil {
		return nil, fmt.Errorf("no vSphere cloud provider config file given")
	}
	cfg := &Config{}
	if err := gcfg.FatalOnly(gcfg.ReadInto(cfg, config)); err != nil {
		return nil, err
	}
	// Env Vars should override config file entries if present
	if err := FromEnv(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// GetCnsconfig returns Config from specified config file path
func GetCnsconfig(cfgPath string) (*Config, error) {
	klog.V(4).Infof("GetCnsconfig called with cfgPath: %s", cfgPath)
	var cfg *Config
	//Read in the vsphere.conf if it exists
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		klog.V(2).Infof("Could not stat %s, reading config params from env", cfgPath)
		// config from Env var only
		cfg = &Config{}
		if fromEnvErr := FromEnv(cfg); fromEnvErr != nil {
			klog.Errorf("Failed to get config params from env. Err: %v", fromEnvErr)
			return cfg, err
		}
	} else {
		config, err := os.Open(cfgPath)
		if err != nil {
			klog.Errorf("Failed to open %s. Err: %v", cfgPath, err)
			return cfg, err
		}
		cfg, err = ReadConfig(config)
		if err != nil {
			klog.Errorf("Failed to parse config. Err: %v", err)
			return cfg, err
		}
	}
	return cfg, nil
}
