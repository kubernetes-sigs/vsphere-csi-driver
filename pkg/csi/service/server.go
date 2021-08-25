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

package service

import (
	"net"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"

	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
)

var (
	stopOnce sync.Once
)

// NonBlockingGRPCServer defines non-blocking GRPC server interfaces.
type NonBlockingGRPCServer interface {
	// Start services at the endpoint.
	Start(endpoint string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer)

	// Stop stops the gRPC server. It immediately closes all open connections
	// and listeners. It cancels all active RPCs on the server side and the
	// corresponding pending RPCs on the client side will get notified by
	// connection errors.
	Stop()

	// GracefulStop stops the gRPC server gracefully. It stops the server
	// from accepting new connections and RPCs and blocks until all the
	// pending RPCs are finished.
	GracefulStop()
}

// NewNonBlockingGRPCServer returns an instance of nonBlockingGRPCServer.
func NewNonBlockingGRPCServer() NonBlockingGRPCServer {
	return &nonBlockingGRPCServer{}
}

// nonBlockingGRPCServer implements the interface NonBlockingGRPCServer.
type nonBlockingGRPCServer struct {
	server *grpc.Server
}

func (s *nonBlockingGRPCServer) Start(endpoint string, ids csi.IdentityServer,
	cs csi.ControllerServer, ns csi.NodeServer) {
	log := logger.GetLoggerWithNoContext()
	if err := s.serve(endpoint, ids, cs, ns); err != nil {
		log.Errorf("failed to start grpc server. Err: %v", err)
	}
}

func (s *nonBlockingGRPCServer) GracefulStop() {
	log := logger.GetLoggerWithNoContext()
	stopOnce.Do(func() {
		if s.server != nil {
			s.server.GracefulStop()
		}
		log.Info("gracefully stopped")
	})
}

func (s *nonBlockingGRPCServer) Stop() {
	log := logger.GetLoggerWithNoContext()
	stopOnce.Do(func() {
		if s.server != nil {
			s.server.Stop()
		}
		log.Info("stopped")
	})
}

func (s *nonBlockingGRPCServer) serve(endpoint string, ids csi.IdentityServer,
	cs csi.ControllerServer, ns csi.NodeServer) error {
	log := logger.GetLoggerWithNoContext()
	u, err := url.Parse(endpoint)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to parse the endpoint %s. Err: %v", endpoint, err)
	}

	// CSI driver currently supports only unix path.
	if u.Scheme != "unix" {
		return logger.LogNewErrorf(log, "endpoint scheme %s not supported", u.Scheme)
	}

	addr := u.Path

	// Remove UNIX sock file if present.
	if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
		return logger.LogNewErrorf(log, "failed to remove %s. Err: %v", addr, err)
	}

	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to listen: %v", err)
	}

	server := grpc.NewServer()
	s.server = server

	// Register the CSI services.
	// Always require the identity service.
	if ids == nil {
		return logger.LogNewError(log, "identity service is required")
	}
	// Either a Controller or Node service should be supplied.
	if cs == nil && ns == nil {
		return logger.LogNewError(log, "either a controller or node service is required")
	}

	// Always register the identity service.
	csi.RegisterIdentityServer(s.server, ids)
	log.Info("identity service registered")

	// Determine which of the controller/node services to register.
	mode := os.Getenv(csitypes.EnvVarMode)
	if strings.EqualFold(mode, "controller") {
		if cs == nil {
			return logger.LogNewError(log, "controller service required when running in controller mode")
		}
		csi.RegisterControllerServer(s.server, cs)
		log.Info("controller service registered")
	} else if strings.EqualFold(mode, "node") {
		if ns == nil {
			return logger.LogNewError(log, "node service required when running in node mode")
		}
		csi.RegisterNodeServer(s.server, ns)
		log.Info("node service registered")
	} else {
		return logger.LogNewErrorf(log, "invalid value %q specified for %s, expecting 'node' or 'controller'",
			mode, csitypes.EnvVarMode)
	}

	log.Infof("Listening for connections on address: %s", listener.Addr())
	if err := server.Serve(listener); err != nil {
		log.Errorf("failed to serve: %v", err)
		return err
	}
	return nil
}
