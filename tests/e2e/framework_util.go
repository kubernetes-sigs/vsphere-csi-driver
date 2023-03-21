/*
Copyright 2023 The Kubernetes Authors.

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

package e2e

import (
	"k8s.io/kubernetes/test/e2e/framework"

	clientset "k8s.io/client-go/kubernetes"

	"github.com/onsi/ginkgo/v2"
	// TODO: Remove the following imports (ref: https://github.com/kubernetes/kubernetes/issues/81245)
)

// NewDefaultFramework makes a new framework and sets up a BeforeEach/AfterEach for
// you (you can write additional before/after each functions).
func NewFramework(baseName string) *framework.Framework {
	options := framework.Options{
		ClientQPS:   20,
		ClientBurst: 50,
	}

	if supervisorCluster {
		return NewSupervisourFramework(baseName, options, nil)
	} else {
		return framework.NewFramework(baseName, options, nil)
	}

}

// NewFramework creates a test framework.
func NewSupervisourFramework(baseName string, options framework.Options, client clientset.Interface) *framework.Framework {
	f := &framework.Framework{
		BaseName:                 baseName,
		SkipNamespaceCreation:    true,
		AddonResourceConstraints: make(map[string]framework.ResourceConstraint),
		Options:                  options,
		ClientSet:                client,
		Timeouts:                 framework.NewTimeoutContextWithDefaults(),
	}

	f.AddAfterEach("dumpNamespaceInfo", func(f *framework.Framework, failed bool) {
		if !failed {
			return
		}
		if !framework.TestContext.DumpLogsOnFailure {
			return
		}
	})

	ginkgo.BeforeEach(f.BeforeEach)
	ginkgo.AfterEach(f.AfterEach)

	return f
}
