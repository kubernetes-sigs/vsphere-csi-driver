/*
Copyright 2025 The Kubernetes Authors.

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

package constants

/*
// test suite labels

flaky -> label include the testcases which fails intermittently
disruptive -> label include the testcases which are disruptive in nature  ex: hosts down, cluster down, datastore down
vanilla -> label include the testcases for block, file, configSecret, topology etc.
stable -> label include the testcases which do not fail
longRunning -> label include the testcases which takes longer time for completion
p0 -> label include the testcases which are P0
p1 -> label include the testcases which are P1, vcreboot, negative
p2 -> label include the testcases which are P2
semiAutomated -> label include the testcases which are semi-automated
newTests -> label include the testcases which are newly automated
core -> label include the testcases specific to block or file
level2 -> label include the level-2 topology testcases or pipeline specific
level5 -> label include the level-5 topology testcases
customPort -> label include the testcases running on vCenter custom port <VC:444>
deprecated ->label include the testcases which are no longer in execution
negative -> Negative tests, ex: service/pod down(sps, vsan-health, vpxd, hostd, csi pods)
vc70 -> Tests for vc70 features
vc80 -> Tests for vc80 features
vc80 -> Tests for vc90 features
vmServiceVm -> vmService VM related testcases
wldi -> Work-Load Domain Isolation testcases
*/
const (
	Flaky                 = "flaky"
	Disruptive            = "disruptive"
	Wcp                   = "wcp"
	Tkg                   = "tkg"
	Vanilla               = "vanilla"
	Preferential          = "preferential"
	VsphereConfigSecret   = "vsphereConfigSecret"
	Snapshot              = "snapshot"
	Stable                = "stable"
	NewTest               = "newTest"
	MultiVc               = "multiVc"
	Block                 = "block"
	File                  = "file"
	Core                  = "core"
	Hci                   = "hci"
	P0                    = "p0"
	P1                    = "p1"
	P2                    = "p2"
	VsanStretch           = "vsanStretch"
	LongRunning           = "longRunning"
	Deprecated            = "deprecated"
	Vmc                   = "vmc"
	TkgsHA                = "tkgsHA"
	ThickThin             = "thickThin"
	CustomPort            = "customPort"
	Windows               = "windows"
	SemiAutomated         = "semiAutomated"
	Level2                = "level2"
	Level5                = "level5"
	Negative              = "negative"
	ListVolume            = "listVolume"
	MultiSvc              = "multiSvc"
	PrimaryCentric        = "primaryCentric"
	ControlPlaneOnPrimary = "controlPlaneOnPrimary"
	Distributed           = "distributed"
	Vmsvc                 = "vmsvc"
	Vc90                  = "vc90"
	Vc80                  = "vc80"
	Vc70                  = "vc70"
	Vc901                 = "vc901"
	Wldi                  = "wldi"
	VmServiceVm           = "vmServiceVm"
	VcptocsiTest          = "vcptocsiTest"
	StretchedSvc          = "stretchedSvc"
	Devops                = "devops"
	LinkedClone           = "lc"
)
