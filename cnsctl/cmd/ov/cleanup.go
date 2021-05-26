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

// cleanupCmd represents the cleanup command.
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Identifies orphan volumes and deletes them",
	Long:  "Identifies orphan volumes and deletes them",
	Run: func(cmd *cobra.Command, args []string) {
		validateOvFlags()
		validateCleanupFlags()
		if len(args) != 0 {
			fmt.Printf("error: no arguments allowed for cleanup\n")
			os.Exit(1)
		}
		// TODO: Add implementation.
	},
}

// InitCleanup help initialize cleanupCmd.
func InitCleanup() {
	cleanupCmd.PersistentFlags().StringVarP(&datastores, "datastores", "d", viper.GetString("datastores"),
		"comma-separated datastore names (alternatively use CNSCTL_DATASTORES env variable)")
	cleanupCmd.PersistentFlags().StringVarP(&cfgFile, "kubeconfig", "k", viper.GetString("kubeconfig"),
		"kubeconfig file (alternatively use CNSCTL_KUBECONFIG env variable)")
	cleanupCmd.PersistentFlags().BoolVarP(&forceDelete, "force", "f", false, "force delete the volumes")
	ovCmd.AddCommand(cleanupCmd)
}

func validateCleanupFlags() {
	if datastores == "" {
		fmt.Printf("error: datastores flag or CNSCTL_DATASTORES env variable must be set for 'cleanup' sub-command\n")
		os.Exit(1)
	}
	if cfgFile == "" {
		fmt.Println("error: kubeconfig flag or CNSCTL_KUBECONFIG env variable not set for 'cleanup' sub-command")
		os.Exit(1)
	}
}
