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

import "time"

const (
	Poll                                     = 2 * time.Second
	PollTimeout                              = 10 * time.Minute
	PollTimeoutShort                         = 1 * time.Minute
	PollTimeoutSixMin                        = 6 * time.Minute
	HealthStatusPollTimeout                  = 20 * time.Minute
	HealthStatusPollInterval                 = 30 * time.Second
	ShortProvisionerTimeout                  = "10"
	PsodTime                                 = "120"
	K8sPodTerminationTimeOut                 = 7 * time.Minute
	K8sPodTerminationTimeOutLong             = 10 * time.Minute
	TotalResizeWaitPeriod                    = 10 * time.Minute
	WaitTimeForCNSNodeVMAttachmentReconciler = 30 * time.Second
	SvOperationTimeout                       = 240 * time.Second
	SupervisorClusterOperationsTimeout       = 3 * time.Minute
	StoragePolicyUsagePollInterval           = 10 * time.Second
	ResizePollInterval                       = 2 * time.Second
	StoragePolicyUsagePollTimeout            = 1 * time.Minute
	SleepTimeOut                             = 30
	MmStateChangeTimeout                     = 300 // int
	OneMinuteWaitTimeInSeconds               = 60
	DefaultFullSyncIntervalInMin             = "30"
	DefaultProvisionerTimeInSec              = "300"
	DefaultFullSyncWaitTime                  = 1800
	DefaultPandoraSyncWaitTime               = 90
	DefaultK8sNodesUpWaitTime                = 25
	HealthStatusWaitTime                     = 3 * time.Minute
	KubeAPIRecoveryTime                      = 1 * time.Minute
	VpxdReducedTaskTimeoutSecsInt            = 90
	VcSessionWaitTime                        = 5 * time.Minute
	PwdRotationTimeout                       = 10 * time.Minute
	PreferredDatastoreRefreshTimeInterval    = 1
	PreferredDatastoreTimeOutInterval        = 1 * time.Minute
)
