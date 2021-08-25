/*
Copyright 2021 The Kubernetes Authors.

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

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"sigs.k8s.io/vsphere-csi-driver/v2/cnsctl/cmd/ov"
	"sigs.k8s.io/vsphere-csi-driver/v2/cnsctl/cmd/ova"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cnsctl",
	Short: "CLI tool for CNS-CSI.",
	Long:  "A fast CLI based tool for storage operations on Cloud Native Storage solution in VMware vSphere.",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func initViper() {
	viper.SetEnvPrefix("cnsctl")
	err := viper.BindEnv("datacenter")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = viper.BindEnv("datastores")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = viper.BindEnv("kubeconfig")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	viper.AutomaticEnv() // read in environment variables that match
}

// InitRoot helps initialize cntctl packages
func InitRoot(version string) {
	initViper()
	rootCmd.Version = version
	ov.InitOv(rootCmd)
	ova.InitOva(rootCmd)
}
