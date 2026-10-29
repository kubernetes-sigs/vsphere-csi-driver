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

package config

import (
	"sync"
	"testing"
)

func TestConfigurationInfoGetSet(t *testing.T) {
	first := &Config{}
	info := NewConfigurationInfo(first)
	if info.GetCfg() != first {
		t.Fatalf("GetCfg must return the config passed to NewConfigurationInfo")
	}
	second := &Config{}
	info.SetCfg(second)
	if info.GetCfg() != second {
		t.Fatalf("GetCfg must return the config set by SetCfg")
	}
	if (&ConfigurationInfo{}).GetCfg() != nil {
		t.Fatalf("zero value ConfigurationInfo must hold a nil config")
	}
}

// TestConfigurationInfoConcurrentAccess is meaningful under the race detector: a config reload swaps the config
// while reconcilers read it.
func TestConfigurationInfoConcurrentAccess(t *testing.T) {
	info := NewConfigurationInfo(&Config{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				info.SetCfg(&Config{})
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = info.GetCfg().Global.ClusterID
			}
		}()
	}
	wg.Wait()
}
