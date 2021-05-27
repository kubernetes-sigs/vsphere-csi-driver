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

package ov

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var datastores, cfgFile string
var all, long bool

// lsCmd represents the ls command.
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List Orphan volumes",
	Long:  "List Orphan volumes",
	Run: func(cmd *cobra.Command, args []string) {
		validateOvFlags()
		validateLsFlags()
		// TODO: Add implementation.
	},
}

// InitLs helps initialize lsCmd.
func InitLs() {
	lsCmd.PersistentFlags().StringVarP(&datastores, "datastores", "d", viper.GetString("datastores"),
		"comma-separated datastore names (alternatively use CNSCTL_DATASTORES env variable)")
	lsCmd.PersistentFlags().StringVarP(&cfgFile, "kubeconfig", "k", viper.GetString("kubeconfig"),
		"comma-separated kubeconfig file(s) (alternatively use CNSCTL_KUBECONFIG env variable)")
	lsCmd.PersistentFlags().BoolVarP(&all, "all", "a", false, "Show orphan and used volumes")
	lsCmd.PersistentFlags().BoolVarP(&long, "long-list", "l", false, "Show additional details of the volumes")
	ovCmd.AddCommand(lsCmd)
}

func validateLsFlags() {
	if datastores == "" {
		fmt.Printf("error: datastores flag or CNSCTL_DATASTORES env variable must be set for 'ls' sub-command\n")
		os.Exit(1)
	}
	if cfgFile == "" {
		fmt.Println("error: kubeconfig flag or CNSCTL_KUBECONFIG env variable not set for 'ls' sub-command")
		os.Exit(1)
	}
}
