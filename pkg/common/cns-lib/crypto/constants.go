/*
Copyright 2024 The Kubernetes Authors.

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

package crypto

import (
	"fmt"
)

const (
	// PVCEncryptionClassAnnotationName is a PVC annotation indicating the associated EncryptionClass
	PVCEncryptionClassAnnotationName = "csi.vsphere.encryption-class"

	// DefaultEncryptionClassLabelName is the name of the label that identifies
	// the default EncryptionClass in a given namespace.
	DefaultEncryptionClassLabelName = "encryption.vmware.com/default"

	// DefaultEncryptionClassLabelValue is the value of the label that
	// identifies the default EncryptionClass in a given namespace.
	DefaultEncryptionClassLabelValue = "true"
)

var (
	// ErrDefaultEncryptionClassNotFound is returned if there are no
	// EncryptionClasses in a given namespace marked as default.
	ErrDefaultEncryptionClassNotFound = fmt.Errorf(
		"no EncryptionClass resource has the label %q: %q",
		DefaultEncryptionClassLabelName, DefaultEncryptionClassLabelValue)

	// ErrMultipleDefaultEncryptionClasses is returned if more than one
	// EncryptionClass in a given namespace are marked as default.
	ErrMultipleDefaultEncryptionClasses = fmt.Errorf(
		"multiple EncryptionClass resources have the label %q: %q",
		DefaultEncryptionClassLabelName, DefaultEncryptionClassLabelValue)
)
