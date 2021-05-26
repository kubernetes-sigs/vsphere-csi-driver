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

var datacenter, vcHost, vcUser, vcPwd string

// ovCmd represents the ov command.
var ovCmd = &cobra.Command{
	Use:   "ov",
	Short: "Orphan volume commands",
	Long:  "Orphan volume commands",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("error: specify one of the subcommands of ov")
		os.Exit(1)
	},
}

// InitOv helps initialize ovCmd.
func InitOv(rootCmd *cobra.Command) {
	InitLs()
	InitRm()
	InitCleanup()

	ovCmd.PersistentFlags().StringVarP(&vcHost, "host", "H", viper.GetString("host"),
		"vCenter host (alternatively use CNSCTL_HOST env variable)")
	ovCmd.PersistentFlags().StringVarP(&vcUser, "user", "u", viper.GetString("user"),
		"vCenter user (alternatively use CNSCTL_USER env variable)")
	ovCmd.PersistentFlags().StringVarP(&vcPwd, "password", "p", viper.GetString("password"),
		"vCenter password (alternatively use CNSCTL_PASSWORD env variable)")
	ovCmd.PersistentFlags().StringVarP(&datacenter, "datacenter", "D", viper.GetString("datacenter"),
		"datacenter name (alternatively use CNSCTL_DATACENTER env variable)")

	rootCmd.AddCommand(ovCmd)
}

func validateOvFlags() {
	if vcHost == "" {
		fmt.Printf("error: host flag or CNSCTL_HOST env variable must be set for 'ov' command\n")
		os.Exit(1)
	}
	if vcUser == "" {
		fmt.Printf("error: user flag or CNSCTL_USER env variable must be set for 'ov' command\n")
		os.Exit(1)
	}
	if vcPwd == "" {
		fmt.Printf("error: password flag or CNSCTL_PASSWORD env variable must be set for 'ov' command\n")
		os.Exit(1)
	}
	if datacenter == "" {
		fmt.Printf("error: datacenter flag or CNSCTL_DATACENTER env variable must be set for 'ov' command\n")
		os.Exit(1)
	}
}
