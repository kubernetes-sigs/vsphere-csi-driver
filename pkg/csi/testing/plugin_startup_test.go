/*
Copyright 2019 The Kubernetes Authors.

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

package test

import (
	"context"
	"net"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/akutz/memconn"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rexray/gocsi"
	"google.golang.org/grpc"

	"k8s.io/cloud-provider-vsphere/pkg/csi/provider"
	"k8s.io/cloud-provider-vsphere/pkg/csi/service"
)

var _ = Describe("CSI plugin", func() {

	var (
		err     error
		ctx     context.Context
		stopSrv func()
		sp      gocsi.StoragePluginProvider
		gclient *grpc.ClientConn
	)

	BeforeEach(func() {
		sp = provider.New()
	})
	AfterEach(func() {
		gclient.Close()
		stopSrv()
	})

	Describe("Started with memory pipe", func() {
		BeforeEach(func() {
			ctx = context.Background()
		})

		When("Plugin unconfigured", func() {
			BeforeEach(func() {
				sp.(*gocsi.StoragePlugin).BeforeServe = nil
			})
			JustBeforeEach(func() {
				gclient, stopSrv, err = getPipedClient(ctx, sp)
				Ω(err).ShouldNot(HaveOccurred())
			})

			Context("Identity Service", func() {
				var client csi.IdentityClient

				JustBeforeEach(func() {
					client = csi.NewIdentityClient(gclient)
				})

				Context("GetPluginInfo", func() {
					var res *csi.GetPluginInfoResponse
					It("should be valid", func() {
						res, err = client.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(res).ShouldNot(BeNil())
						Ω(res.GetName()).Should(Equal(service.Name))
					})
				})
				Context("GetPluginCapabilities", func() {
					var res *csi.GetPluginCapabilitiesResponse
					It("should have correct caps listed", func() {
						res, err = client.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(res).ShouldNot(BeNil())
						caps := res.GetCapabilities()
						Ω(caps).Should(HaveLen(2))
						svcTypes := []csi.PluginCapability_Service_Type{
							caps[0].GetService().Type,
							caps[1].GetService().Type,
						}
						Ω(svcTypes).Should(ConsistOf(
							csi.PluginCapability_Service_CONTROLLER_SERVICE,
							csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS))
					})
				})
				PContext("Probe", func() {
					It("Should fail", func() {
						_, err = client.Probe(ctx, &csi.ProbeRequest{})
						Ω(err).Should(HaveOccurred())
						//TODO check that the gRPC status code is FAILED_PRECONDITION
					})
				})
			})
			Context("Controller Service", func() {
				var client csi.ControllerClient

				JustBeforeEach(func() {
					client = csi.NewControllerClient(gclient)
				})

				Context("GetCapabilities", func() {
					var res *csi.ControllerGetCapabilitiesResponse
					It("should be valid", func() {
						res, err = client.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(res).ShouldNot(BeNil())
						caps := res.GetCapabilities()
						Ω(caps).Should(HaveLen(3))
						rpcTypes := []csi.ControllerServiceCapability_RPC_Type{
							caps[0].GetRpc().Type,
							caps[1].GetRpc().Type,
							caps[2].GetRpc().Type,
						}
						Ω(rpcTypes).Should(ConsistOf(
							csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
							csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
							csi.ControllerServiceCapability_RPC_LIST_VOLUMES))
					})
				})
			})
		})
	})

})

// getPipedClient serves the given SP over an in-memory pipe
func getPipedClient(ctx context.Context, sp gocsi.StoragePluginProvider) (*grpc.ClientConn, func(), error) {

	lis, err := memconn.Listen("memu", "csi-vsphere-test")
	Ω(err).Should(BeNil())
	go func() {
		defer GinkgoRecover()
		if err := sp.Serve(ctx, lis); err != nil {
			Ω(err.Error()).Should(Equal("http: Server closed"))
		}
	}()

	clientOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return memconn.Dial("memu", "csi-vsphere-test")
		}),
	}

	// Create a client for the piped connection.
	client, err := grpc.DialContext(ctx, "", clientOpts...)
	Ω(err).ShouldNot(HaveOccurred())

	return client, func() { sp.GracefulStop(ctx) }, nil
}
