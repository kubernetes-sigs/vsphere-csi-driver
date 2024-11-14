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
