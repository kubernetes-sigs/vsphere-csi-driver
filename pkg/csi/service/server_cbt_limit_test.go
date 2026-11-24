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
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// blockingSnapshotMetadataServer holds every CBT stream open until release is
// closed, so the test controls exactly how many sessions are in flight.
type blockingSnapshotMetadataServer struct {
	csi.UnimplementedSnapshotMetadataServer
	started chan struct{}
	release chan struct{}
}

func (f *blockingSnapshotMetadataServer) wait(ctx context.Context) error {
	f.started <- struct{}{}
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *blockingSnapshotMetadataServer) GetMetadataAllocated(_ *csi.GetMetadataAllocatedRequest,
	srv csi.SnapshotMetadata_GetMetadataAllocatedServer) error {
	if err := f.wait(srv.Context()); err != nil {
		return err
	}
	return srv.Send(&csi.GetMetadataAllocatedResponse{VolumeCapacityBytes: 1})
}

func (f *blockingSnapshotMetadataServer) GetMetadataDelta(_ *csi.GetMetadataDeltaRequest,
	srv csi.SnapshotMetadata_GetMetadataDeltaServer) error {
	if err := f.wait(srv.Context()); err != nil {
		return err
	}
	return srv.Send(&csi.GetMetadataDeltaResponse{VolumeCapacityBytes: 1})
}

// recvCode drains a stream and returns the final gRPC status code
// (codes.OK when the stream ends cleanly with io.EOF).
func recvCode(recv func() error) codes.Code {
	for {
		err := recv()
		if err == io.EOF {
			return codes.OK
		}
		if err != nil {
			return status.Code(err)
		}
	}
}

// TestCBTSessionLimitOnRealGRPCServer builds the production gRPC server via
// newGRPCServer, serves it on a real unix socket and drives it with a real
// gRPC client. This verifies the CBT limiter is wired into the server, picks
// the limit for the current cluster flavor, rejects sessions over the limit
// with ResourceExhausted, leaves other services alone, and frees slots once
// streams finish.
func TestCBTSessionLimitOnRealGRPCServer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flavor cnstypes.CnsClusterFlavor
		svVal  string
		gcVal  string
		limit  int
	}{
		{"supervisor", cnstypes.CnsClusterFlavorWorkload, "2", "5", 2},
		{"guest", cnstypes.CnsClusterFlavorGuest, "5", "1", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origFlavor := clusterFlavor
			t.Cleanup(func() { clusterFlavor = origFlavor })
			clusterFlavor = tc.flavor
			t.Setenv(csitypes.EnvVarMode, "controller")
			t.Setenv(csitypes.EnvSupervisorMaxConcurrentCBTSessions, tc.svVal)
			t.Setenv(csitypes.EnvGuestMaxConcurrentCBTSessions, tc.gcVal)

			sms := &blockingSnapshotMetadataServer{
				// Extra capacity covers the post-release retries below.
				started: make(chan struct{}, tc.limit+64),
				release: make(chan struct{}),
			}
			srv, err := newGRPCServer(&csi.UnimplementedIdentityServer{},
				&csi.UnimplementedControllerServer{}, nil, sms)
			if err != nil {
				t.Fatalf("newGRPCServer: %v", err)
			}
			sock := filepath.Join(t.TempDir(), "csi.sock")
			lis, err := net.Listen("unix", sock)
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)

			conn, err := grpc.NewClient("unix://"+sock,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			client := csi.NewSnapshotMetadataClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Fill every slot with a held GetMetadataDelta stream.
			held := make(chan codes.Code, tc.limit)
			for i := 0; i < tc.limit; i++ {
				stream, err := client.GetMetadataDelta(ctx, &csi.GetMetadataDeltaRequest{})
				if err != nil {
					t.Fatalf("open held stream %d: %v", i, err)
				}
				go func() {
					held <- recvCode(func() error { _, err := stream.Recv(); return err })
				}()
				select {
				case <-sms.started:
				case <-ctx.Done():
					t.Fatalf("held stream %d never reached the handler", i)
				}
			}

			// Both CBT methods share the limit and are rejected when full.
			// Rejection is immediate, so a short timeout makes a missing
			// limiter fail fast (as DeadlineExceeded) instead of hanging.
			rejCtx, rejCancel := context.WithTimeout(ctx, 5*time.Second)
			defer rejCancel()
			delta, err := client.GetMetadataDelta(rejCtx, &csi.GetMetadataDeltaRequest{})
			if err != nil {
				t.Fatalf("open delta stream: %v", err)
			}
			if c := recvCode(func() error { _, err := delta.Recv(); return err }); c != codes.ResourceExhausted {
				t.Fatalf("GetMetadataDelta over limit: got %v, want ResourceExhausted", c)
			}
			alloc, err := client.GetMetadataAllocated(rejCtx, &csi.GetMetadataAllocatedRequest{})
			if err != nil {
				t.Fatalf("open allocated stream: %v", err)
			}
			if c := recvCode(func() error { _, err := alloc.Recv(); return err }); c != codes.ResourceExhausted {
				t.Fatalf("GetMetadataAllocated over limit: got %v, want ResourceExhausted", c)
			}

			// Other services are not limited (the fakes are unimplemented).
			_, err = csi.NewIdentityClient(conn).GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
			if c := status.Code(err); c != codes.Unimplemented {
				t.Fatalf("GetPluginInfo while CBT is full: got %v, want Unimplemented", c)
			}

			// Release the held streams; they complete successfully.
			close(sms.release)
			for i := 0; i < tc.limit; i++ {
				if c := <-held; c != codes.OK {
					t.Fatalf("held stream finished with %v, want OK", c)
				}
			}

			// Slots are free again, so a new session is accepted. The slot is
			// released just after the client sees EOF, so allow a brief retry.
			var last codes.Code
			for attempt := 0; attempt < 50; attempt++ {
				stream, err := client.GetMetadataAllocated(ctx, &csi.GetMetadataAllocatedRequest{})
				if err != nil {
					t.Fatalf("open stream after release: %v", err)
				}
				last = recvCode(func() error { _, err := stream.Recv(); return err })
				if last == codes.OK {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("session after release: got %v, want OK", last)
		})
	}
}
