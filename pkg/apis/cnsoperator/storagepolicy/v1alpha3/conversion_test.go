// Copyright (c) 2026 Broadcom. All Rights Reserved.
// Broadcom Confidential. The term "Broadcom" refers to Broadcom Inc.
// and/or its subsidiaries.
// SPDX-License-Identifier: Apache-2.0

package v1alpha3_test

import (
	"testing"

	ctrlconversion "sigs.k8s.io/controller-runtime/pkg/conversion"
	v1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
)

// Hub methods are markers for controller-runtime conversion; call them so coverage
// attributes the hub registration surface.
func TestHubInterfaces(t *testing.T) {
	var (
		_ ctrlconversion.Hub = (*v1alpha3.StorageQuota)(nil)
		_ ctrlconversion.Hub = (*v1alpha3.StorageQuotaList)(nil)
		_ ctrlconversion.Hub = (*v1alpha3.StoragePolicyQuota)(nil)
		_ ctrlconversion.Hub = (*v1alpha3.StoragePolicyQuotaList)(nil)
		_ ctrlconversion.Hub = (*v1alpha3.StoragePolicyUsage)(nil)
		_ ctrlconversion.Hub = (*v1alpha3.StoragePolicyUsageList)(nil)
	)
	(*v1alpha3.StorageQuota)(nil).Hub()
	(*v1alpha3.StorageQuotaList)(nil).Hub()
	(*v1alpha3.StoragePolicyQuota)(nil).Hub()
	(*v1alpha3.StoragePolicyQuotaList)(nil).Hub()
	(*v1alpha3.StoragePolicyUsage)(nil).Hub()
	(*v1alpha3.StoragePolicyUsageList)(nil).Hub()
}
