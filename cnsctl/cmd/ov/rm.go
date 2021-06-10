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

var datastore string
var forceDelete bool

// rmCmd represents the rm command.
var rmCmd = &cobra.Command{
	Use:   "rm",
	Short: "Remove specified volume IDs",
	Long:  "Remove specified volume IDs",
	Run: func(cmd *cobra.Command, args []string) {
		validateOvFlags()
		validateRmFlags()
		if len(args) == 0 {
			fmt.Printf("error: no volumes specified to be deleted.\n")
			os.Exit(1)
		}
		// TODO: Add implementation.
	},
}

// InitRm helps initialize rmCmd.
func InitRm() {
	rmCmd.PersistentFlags().StringVarP(&datastore, "datastore", "d", "", "a single datastore name")
	rmCmd.PersistentFlags().BoolVarP(&forceDelete, "force", "f", false, "force delete the volume")
	rmCmd.PersistentFlags().StringVarP(&cfgFile, "kubeconfig", "k", viper.GetString("kubeconfig"),
		"kubeconfig file (alternatively use CNSCTL_KUBECONFIG env variable)")
	ovCmd.AddCommand(rmCmd)
}

func validateRmFlags() {
	if datastore == "" {
		fmt.Printf("error: datastore flag must be set for 'rm' sub-command\n")
		os.Exit(1)
	}
	if cfgFile == "" {
		fmt.Println("error: kubeconfig flag or CNSCTL_KUBECONFIG env variable not set for 'rm' sub-command")
		os.Exit(1)
	}
}
