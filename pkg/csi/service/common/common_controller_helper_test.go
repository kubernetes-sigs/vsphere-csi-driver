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

package common

import (
	"context"
	"testing"
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotfake "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	vim25types "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestUseVslmAPIsFuncForVC67Update3l tests UseVslmAPIs method for VC version 6.7 Update 3l
func TestUseVslmAPIsFuncForVC67Update3l(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Scenario 1: VC version in 6.7 Update 3l ==> UseVslmAPIs, should return true
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 6.7.0 build-17137327",
		Vendor:                "VMware, Inc.",
		Version:               "6.7.0",
		Build:                 "17137327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "6.7.3",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "6.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if !bUseVslm {
			t.Fatal("UseVslmAPIs returned false, expecting true")
		}
	}
}

// TestUseVslmAPIsFuncForVC67Update3a tests UseVslmAPIs method for 6.7 Update 3a
func TestUseVslmAPIsFuncForVC67Update3a(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 2: VC version in 6.7 Update 3a ==> UseVslmAPIs, should return false
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 6.7.0 build-17137327",
		Vendor:                "VMware, Inc.",
		Version:               "6.7.0",
		Build:                 "17037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "6.7.3",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "6.0",
	}
	_, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		t.Errorf("error expected but not received. aboutInfo: %+v", aboutInfo)
	}
	t.Logf("expected err received. err: %v", err)
}

// TestUseVslmAPIsFuncForVC6dot6 tests UseVslmAPIs method for VC version 6.6
func TestUseVslmAPIsFuncForVC6dot6(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 3: VC version in 6.6 ==> UseVslmAPIs, should return false
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 6.6.0 build-18037327",
		Vendor:                "VMware, Inc.",
		Version:               "6.6.0",
		Build:                 "18037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "6.6.0",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "6.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if bUseVslm {
			t.Fatal("UseVslmAPIs returned true, expecting false")
		}
	} else {
		t.Fatal("Received error from UseVslmAPIs method")
	}
}

// TestUseVslmAPIsFuncForVC7 tests UseVslmAPIs method for VC version 7.0
func TestUseVslmAPIsFuncForVC7(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 4: VC version in 7.0 ==> UseVslmAPIs, should return true
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 7.0.0.0 build-19037327",
		Vendor:                "VMware, Inc.",
		Version:               "7.0.0.0",
		Build:                 "19037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "7.0.0.0",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "7.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if !bUseVslm {
			t.Fatal("UseVslmAPIs returned false, expecting true")
		}
	} else {
		t.Fatal("Received error from UseVslmAPIs method")
	}
}

// TestUseVslmAPIsFuncForVC70u1 tests UseVslmAPIs method for VC version 70u1
func TestUseVslmAPIsFuncForVC70u1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Scenario 5: VC version in 7.0 Update 1 ==> UseVslmAPIs, should return false
	// CSI migration is supported using CNS APIs
	aboutInfo := vim25types.AboutInfo{
		Name:                  "VMware vCenter Server",
		FullName:              "VMware vCenter Server 7.0.1.0 build-19237327",
		Vendor:                "VMware, Inc.",
		Version:               "7.0.1.0",
		Build:                 "19037327",
		LocaleVersion:         "INTL",
		LocaleBuild:           "000",
		OsType:                "linux-x64",
		ProductLineId:         "vpx",
		ApiType:               "VirtualCenter",
		ApiVersion:            "7.0.1.0",
		InstanceUuid:          "4394cd59-cd63-43df-bdf5-8a2cee4fe055",
		LicenseProductName:    "VMware VirtualCenter Server",
		LicenseProductVersion: "7.0",
	}
	bUseVslm, err := UseVslmAPIs(ctx, aboutInfo)
	if err == nil {
		if bUseVslm {
			t.Fatal("UseVslmAPIs returned true, expecting false")
		}
	} else {
		t.Fatal("Received error from UseVslmAPIs method")
	}
}

// TestCheckAPIForVC8 tests CheckAPI method for VC version 8.
func TestCheckAPIForVC8(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vcVersion := "8.0.0.1.0"
	err := CheckAPI(ctx, vcVersion, MinSupportedVCenterMajor, MinSupportedVCenterMinor,
		MinSupportedVCenterPatch)
	if err != nil {
		t.Fatalf("CheckAPI method failing for VC %q", vcVersion)
	}
}

// TestCheckAPIForVC70u3 tests CheckAPI method for VC version 7.0U3.
func TestCheckAPIForVC70u3(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vcVersion := "7.0.3.0"
	err := CheckAPI(ctx, vcVersion, MinSupportedVCenterMajor, MinSupportedVCenterMinor,
		MinSupportedVCenterPatch)
	if err != nil {
		t.Fatalf("CheckAPI method failing for VC %q", vcVersion)
	}
}

// TestWaitForPVCDeletedWithWatch tests the WaitForPVCDeletedWithWatch function
func TestWaitForPVCDeletedWithWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test case 1: PVC already deleted (not found)
	t.Run("PVC already deleted", func(t *testing.T) {
		k8sclient := fake.NewSimpleClientset()

		err := WaitForPVCDeletedWithWatch(ctx, k8sclient, "test-pvc", "test-namespace", time.Second*5)
		if err != nil {
			t.Fatalf("Expected no error when PVC is already deleted, got: %v", err)
		}
	})

	// Test case 2: PVC exists and gets deleted during watch
	t.Run("PVC gets deleted during watch", func(t *testing.T) {
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		}
		k8sclient := fake.NewSimpleClientset(pvc)

		// Start watching in a goroutine
		errChan := make(chan error, 1)
		go func() {
			err := WaitForPVCDeletedWithWatch(ctx, k8sclient, "test-pvc", "test-namespace", time.Second*5)
			errChan <- err
		}()

		// Give the watch time to start
		time.Sleep(time.Millisecond * 100)

		// Delete the PVC
		err := k8sclient.CoreV1().PersistentVolumeClaims("test-namespace").Delete(ctx, "test-pvc", metav1.DeleteOptions{})
		if err != nil {
			t.Fatalf("Failed to delete PVC: %v", err)
		}

		// Wait for the watch to complete
		select {
		case err := <-errChan:
			if err != nil {
				t.Fatalf("Expected no error when PVC is deleted during watch, got: %v", err)
			}
		case <-time.After(time.Second * 2):
			t.Fatal("Watch did not complete within expected time")
		}
	})

	// Test case 3: Watch timeout
	t.Run("Watch timeout", func(t *testing.T) {
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		}
		k8sclient := fake.NewSimpleClientset(pvc)

		err := WaitForPVCDeletedWithWatch(ctx, k8sclient, "test-pvc", "test-namespace", time.Millisecond*100)
		if err == nil {
			t.Fatal("Expected timeout error when PVC is not deleted")
		}
	})
}

// TestWaitForVolumeSnapshotDeletedWithWatch tests the WaitForVolumeSnapshotDeletedWithWatch function
func TestWaitForVolumeSnapshotDeletedWithWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test case 1: VolumeSnapshot already deleted (not found)
	t.Run("VolumeSnapshot already deleted", func(t *testing.T) {
		snapshotClient := snapshotfake.NewSimpleClientset()

		err := WaitForVolumeSnapshotDeletedWithWatch(ctx, snapshotClient, "test-snapshot", "test-namespace", time.Second*5)
		if err != nil {
			t.Fatalf("Expected no error when VolumeSnapshot is already deleted, got: %v", err)
		}
	})

	// Test case 2: VolumeSnapshot exists and gets deleted during watch
	t.Run("VolumeSnapshot gets deleted during watch", func(t *testing.T) {
		snapshot := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-snapshot",
				Namespace: "test-namespace",
			},
		}
		snapshotClient := snapshotfake.NewSimpleClientset(snapshot)

		// Start watching in a goroutine
		errChan := make(chan error, 1)
		go func() {
			err := WaitForVolumeSnapshotDeletedWithWatch(ctx, snapshotClient, "test-snapshot", "test-namespace", time.Second*5)
			errChan <- err
		}()

		// Give the watch time to start
		time.Sleep(time.Millisecond * 100)

		// Delete the VolumeSnapshot
		err := snapshotClient.SnapshotV1().VolumeSnapshots("test-namespace").Delete(
			ctx, "test-snapshot", metav1.DeleteOptions{})
		if err != nil {
			t.Fatalf("Failed to delete VolumeSnapshot: %v", err)
		}

		// Wait for the watch to complete
		select {
		case err := <-errChan:
			if err != nil {
				t.Fatalf("Expected no error when VolumeSnapshot is deleted during watch, got: %v", err)
			}
		case <-time.After(time.Second * 2):
			t.Fatal("Watch did not complete within expected time")
		}
	})

	// Test case 3: Watch timeout
	t.Run("Watch timeout", func(t *testing.T) {
		snapshot := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-snapshot",
				Namespace: "test-namespace",
			},
		}
		snapshotClient := snapshotfake.NewSimpleClientset(snapshot)

		err := WaitForVolumeSnapshotDeletedWithWatch(
			ctx, snapshotClient, "test-snapshot", "test-namespace", time.Millisecond*100)
		if err == nil {
			t.Fatal("Expected timeout error when VolumeSnapshot is not deleted")
		}
	})
}
