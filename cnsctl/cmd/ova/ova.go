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
)

// ovaCmd represents the ova command
var ovaCmd = &cobra.Command{
	Use:   "ova",
	Short: "Commands on orphan volume attachment CRs in Kubernetes",
	Long:  "Commands on orphan volume attachment CRs in Kubernetes",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("error: specify one of the subcommands of ova")
		os.Exit(1)
	},
}

// InitOva helps initialize ova command
func InitOva(rootCmd *cobra.Command) {
	InitLs()
	InitCleanup()
	rootCmd.AddCommand(ovaCmd)
}
