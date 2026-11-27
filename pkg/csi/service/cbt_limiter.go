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
	"os"
	"strconv"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

const (
	getMetadataAllocatedFullMethod = "/csi.v1.SnapshotMetadata/GetMetadataAllocated"
	getMetadataDeltaFullMethod     = "/csi.v1.SnapshotMetadata/GetMetadataDelta"
)

// cbtSessionLimiter caps the number of concurrently served CBT streaming
// RPCs. Sessions over the cap are rejected immediately with ResourceExhausted
// rather than queued, so streams cannot pile up inside the driver.
type cbtSessionLimiter struct {
	slots chan struct{}
}

func newCBTSessionLimiter(limit int) *cbtSessionLimiter {
	return &cbtSessionLimiter{slots: make(chan struct{}, limit)}
}

func isCBTMethod(fullMethod string) bool {
	return fullMethod == getMetadataAllocatedFullMethod || fullMethod == getMetadataDeltaFullMethod
}

func (l *cbtSessionLimiter) streamInterceptor(srv interface{}, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !isCBTMethod(info.FullMethod) {
		return handler(srv, ss)
	}
	select {
	case l.slots <- struct{}{}:
		prometheus.CsiCBTSessionsInFlightGauge.Inc()
		defer func() {
			<-l.slots
			prometheus.CsiCBTSessionsInFlightGauge.Dec()
		}()
		return handler(srv, ss)
	default:
		prometheus.CsiCBTSessionsRejectedCounterVec.WithLabelValues(info.FullMethod).Inc()
		logger.GetLoggerWithNoContext().Warnf(
			"rejecting %s: maximum concurrent CBT sessions (%d) reached", info.FullMethod, cap(l.slots))
		return status.Errorf(codes.ResourceExhausted,
			"maximum number of concurrent CBT sessions (%d) reached, retry later", cap(l.slots))
	}
}

// getMaxConcurrentCBTSessions returns the CBT session limit for the current
// cluster flavor (Supervisor or Guest), falling back to that flavor's default
// when its env var is unset or invalid.
func getMaxConcurrentCBTSessions(log *zap.SugaredLogger) int {
	envVar := csitypes.EnvSupervisorMaxConcurrentCBTSessions
	def := common.DefaultSupervisorMaxConcurrentCBTSessions
	if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		envVar = csitypes.EnvGuestMaxConcurrentCBTSessions
		def = common.DefaultGuestMaxConcurrentCBTSessions
	}
	v := os.Getenv(envVar)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Warnf("invalid %s=%q, using default %d", envVar, v, def)
		return def
	}
	return n
}
