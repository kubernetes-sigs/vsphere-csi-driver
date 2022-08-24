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

	vim25types "github.com/vmware/govmomi/vim25/types"
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
