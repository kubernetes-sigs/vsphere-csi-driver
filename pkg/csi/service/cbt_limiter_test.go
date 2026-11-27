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

package service

import (
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

func TestCBTSessionLimiter(t *testing.T) {
	const limit = 2
	l := newCBTSessionLimiter(limit)
	info := &grpc.StreamServerInfo{FullMethod: getMetadataDeltaFullMethod}

	release := make(chan struct{})
	started := make(chan struct{}, limit)
	done := make(chan error, limit)
	blocking := func(interface{}, grpc.ServerStream) error {
		started <- struct{}{}
		<-release
		return nil
	}
	for i := 0; i < limit; i++ {
		go func() { done <- l.streamInterceptor(nil, nil, info, blocking) }()
	}
	for i := 0; i < limit; i++ {
		<-started
	}

	noop := func(interface{}, grpc.ServerStream) error { return nil }

	// Over the limit: CBT call rejected immediately.
	err := l.streamInterceptor(nil, nil, info, noop)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	// Non-CBT streams are unaffected.
	other := &grpc.StreamServerInfo{FullMethod: "/csi.v1.Controller/ListVolumes"}
	if err := l.streamInterceptor(nil, nil, other, noop); err != nil {
		t.Fatalf("non-CBT stream should not be limited, got %v", err)
	}

	// Slots are released after completion.
	close(release)
	for i := 0; i < limit; i++ {
		if err := <-done; err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := l.streamInterceptor(nil, nil, info, noop); err != nil {
		t.Fatalf("expected slot to be free, got %v", err)
	}
}

func TestGetMaxConcurrentCBTSessions(t *testing.T) {
	log := logger.GetLoggerWithNoContext()
	orig := clusterFlavor
	defer func() { clusterFlavor = orig }()

	for _, tc := range []struct {
		name   string
		flavor cnstypes.CnsClusterFlavor
		envVar string
		val    string
		want   int
	}{
		{"supervisor default", cnstypes.CnsClusterFlavorWorkload, "", "",
			common.DefaultSupervisorMaxConcurrentCBTSessions},
		{"guest default", cnstypes.CnsClusterFlavorGuest, "", "",
			common.DefaultGuestMaxConcurrentCBTSessions},
		{"supervisor override", cnstypes.CnsClusterFlavorWorkload,
			csitypes.EnvSupervisorMaxConcurrentCBTSessions, "50", 50},
		{"guest override", cnstypes.CnsClusterFlavorGuest,
			csitypes.EnvGuestMaxConcurrentCBTSessions, "70", 70},
		{"supervisor ignores guest var", cnstypes.CnsClusterFlavorWorkload,
			csitypes.EnvGuestMaxConcurrentCBTSessions, "70",
			common.DefaultSupervisorMaxConcurrentCBTSessions},
		{"guest ignores supervisor var", cnstypes.CnsClusterFlavorGuest,
			csitypes.EnvSupervisorMaxConcurrentCBTSessions, "50",
			common.DefaultGuestMaxConcurrentCBTSessions},
		{"invalid", cnstypes.CnsClusterFlavorGuest,
			csitypes.EnvGuestMaxConcurrentCBTSessions, "abc", common.DefaultGuestMaxConcurrentCBTSessions},
		{"zero", cnstypes.CnsClusterFlavorWorkload,
			csitypes.EnvSupervisorMaxConcurrentCBTSessions, "0", common.DefaultSupervisorMaxConcurrentCBTSessions},
		{"negative", cnstypes.CnsClusterFlavorWorkload,
			csitypes.EnvSupervisorMaxConcurrentCBTSessions, "-5", common.DefaultSupervisorMaxConcurrentCBTSessions},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(csitypes.EnvSupervisorMaxConcurrentCBTSessions, "")
			t.Setenv(csitypes.EnvGuestMaxConcurrentCBTSessions, "")
			if tc.envVar != "" {
				t.Setenv(tc.envVar, tc.val)
			}
			clusterFlavor = tc.flavor
			if got := getMaxConcurrentCBTSessions(log); got != tc.want {
				t.Errorf("got %d want %d", got, tc.want)
			}
		})
	}
}
