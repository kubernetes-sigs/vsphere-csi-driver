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

// Package fakesnapshot provides a fake external-snapshotter clientset for unit
// tests. It is a leaf package (no dependencies on other vsphere-csi-driver
// packages) so it can be imported from white-box tests without risking import
// cycles.
package fakesnapshot

import (
	snapshotfake "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"k8s.io/apimachinery/pkg/runtime"
)

// NewClientset returns a fake snapshot clientset for use in unit tests.
//
// external-snapshotter/client/v8 v8.6.0 deprecates the generated
// fake.NewSimpleClientset in favor of a NewClientset constructor that supports
// server-side-apply field management. However, that replacement is only
// generated when the client is built with apply configurations (client-gen
// --apply-configuration-package). external-snapshotter's codegen does not enable
// that, so neither NewClientset nor an applyconfiguration package exist in any
// released v8.x (v8.6.0 is the latest, and current upstream master still calls
// NewSimpleClientset in its own tests). NewSimpleClientset is therefore the only
// available constructor. This shim exposes the recommended NewClientset name so
// callers read as the intended API, and centralizes the single, unavoidable
// staticcheck suppression here with justification.
func NewClientset(objects ...runtime.Object) *snapshotfake.Clientset {
	// external-snapshotter ships no NewClientset replacement (see package doc),
	// so the deprecated constructor is unavoidable. The SA1019 suppression needs
	// two directives because make check runs two tools that honor different ones:
	// //lint:ignore for standalone staticcheck, //nolint for golangci-lint.
	//lint:ignore SA1019 no NewClientset shipped by external-snapshotter; see package doc.
	return snapshotfake.NewSimpleClientset(objects...) //nolint:staticcheck // see package doc
}
