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

package ova

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// cleanupCmd represents the cleanup command.
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Identifies orphan volume attachment CRs and deletes them",
	Long:  "Identifies orphan volume attachment CRs and deletes them",
	Run: func(cmd *cobra.Command, args []string) {
		validateCleanupFlags()

		if len(args) != 0 {
			fmt.Printf("error: no arguments allowed for cleanup\n")
			os.Exit(1)
		}
		// TODO: Add implementation
	},
}

// InitCleanup helps initialize cleanupCmd.
func InitCleanup() {
	cleanupCmd.PersistentFlags().StringVarP(&cfgFile, "kubeconfig", "k", viper.GetString("kubeconfig"),
		"kubeconfig file (alternatively use CNSCTL_KUBECONFIG env variable)")
	ovaCmd.AddCommand(cleanupCmd)
}

func validateCleanupFlags() {
	if cfgFile == "" {
		fmt.Println("error: kubeconfig flag or CNSCTL_KUBECONFIG env variable not set for 'cleanup' sub-command")
		os.Exit(1)
	}
}
