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

package wcpguest

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snapshotmetadataapi "github.com/kubernetes-csi/external-snapshot-metadata/pkg/api"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotterfake "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// testSupervisorSnapshotNamespace is the namespace used for all fake Supervisor VolumeSnapshot
// objects the mock lookup returns. The guest pvCSI forwards this namespace to the Supervisor
// sidecar as-is.
const testSupervisorSnapshotNamespace = "test-sv-ns"

// seedSupervisorVolumeSnapshots installs a fake snapshotter client on the controller with a
// VolumeSnapshot named after every CSI snapshot handle in handles. The guest pvCSI uses a
// namespace-scoped VolumeSnapshot Get on the Supervisor to translate the target snapshot
// handle to (namespace, name); base_snapshot_id is forwarded as the change-id.
func seedSupervisorVolumeSnapshots(t *testing.T, c *controller, handles ...string) {
	t.Helper()
	objs := make([]runtime.Object, 0, len(handles))
	for _, h := range handles {
		objs = append(objs,
			&snapshotv1.VolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      h,
					Namespace: testSupervisorSnapshotNamespace,
				},
			},
		)
	}
	c.supervisorSnapshotterClient = snapshotterfake.NewSimpleClientset(objs...)
	c.supervisorNamespace = testSupervisorSnapshotNamespace
}

// mockSupervisorSnapshotMetadataServer implements snapshotmetadataapi.SnapshotMetadataServer,
// emulating the external-snapshot-metadata sidecar the Supervisor exposes to pvCSI. The driver
// under test speaks this proto, not the csi.v1.SnapshotMetadata one.
type mockSupervisorSnapshotMetadataServer struct {
	snapshotmetadataapi.UnimplementedSnapshotMetadataServer
	allocatedResponses []*snapshotmetadataapi.GetMetadataAllocatedResponse
	deltaResponses     []*snapshotmetadataapi.GetMetadataDeltaResponse
	errToReturn        error
	// afterSendsReturn is returned from the handler after all configured Sends (e.g. OutOfRange for EOF-like end).
	afterSendsReturn error
	// blockAfterSendsUntilCtxDone: after sending all responses, block until server.Context() is done,
	// then return afterSendsReturn (or Canceled).
	blockAfterSendsUntilCtxDone bool
	// lastAllocatedReq / lastDeltaReq capture the last request each handler received so tests can
	// assert on the (namespace, snapshot_name) translation done by the driver.
	lastAllocatedReq *snapshotmetadataapi.GetMetadataAllocatedRequest
	lastDeltaReq     *snapshotmetadataapi.GetMetadataDeltaRequest
}

func (m *mockSupervisorSnapshotMetadataServer) GetMetadataAllocated(
	req *snapshotmetadataapi.GetMetadataAllocatedRequest,
	server snapshotmetadataapi.SnapshotMetadata_GetMetadataAllocatedServer) error {
	m.lastAllocatedReq = req
	if m.errToReturn != nil {
		return m.errToReturn
	}
	md, ok := metadata.FromIncomingContext(server.Context())
	if !ok || len(md["authorization"]) == 0 || md["authorization"][0] != "Bearer test-token" {
		return status.Error(codes.Unauthenticated, "invalid or missing token")
	}
	for _, resp := range m.allocatedResponses {
		if err := server.Send(resp); err != nil {
			return err
		}
	}
	if m.blockAfterSendsUntilCtxDone {
		<-server.Context().Done()
		if m.afterSendsReturn != nil {
			return m.afterSendsReturn
		}
		return status.Error(codes.Canceled, server.Context().Err().Error())
	}
	if m.afterSendsReturn != nil {
		return m.afterSendsReturn
	}
	return nil
}

func (m *mockSupervisorSnapshotMetadataServer) GetMetadataDelta(
	req *snapshotmetadataapi.GetMetadataDeltaRequest,
	server snapshotmetadataapi.SnapshotMetadata_GetMetadataDeltaServer) error {
	m.lastDeltaReq = req
	if m.errToReturn != nil {
		return m.errToReturn
	}
	md, ok := metadata.FromIncomingContext(server.Context())
	if !ok || len(md["authorization"]) == 0 || md["authorization"][0] != "Bearer test-token" {
		return status.Error(codes.Unauthenticated, "invalid or missing token")
	}
	for _, resp := range m.deltaResponses {
		if err := server.Send(resp); err != nil {
			return err
		}
	}
	if m.blockAfterSendsUntilCtxDone {
		<-server.Context().Done()
		if m.afterSendsReturn != nil {
			return m.afterSendsReturn
		}
		return status.Error(codes.Canceled, server.Context().Err().Error())
	}
	if m.afterSendsReturn != nil {
		return m.afterSendsReturn
	}
	return nil
}

// mockAllocatedStreamServer implements csi.SnapshotMetadata_GetMetadataAllocatedServer
type mockAllocatedStreamServer struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*csi.GetMetadataAllocatedResponse
	// failOutgoingAfterN: if > 0, fail the Send call that would be the (N+1)th outbound message (after N successes).
	failOutgoingAfterN int
}

func (m *mockAllocatedStreamServer) Send(resp *csi.GetMetadataAllocatedResponse) error {
	if m.failOutgoingAfterN > 0 && len(m.responses) >= m.failOutgoingAfterN {
		return fmt.Errorf("injected downstream send failure")
	}
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockAllocatedStreamServer) Context() context.Context {
	return m.ctx
}

// mockDeltaStreamServer implements csi.SnapshotMetadata_GetMetadataDeltaServer
type mockDeltaStreamServer struct {
	grpc.ServerStream
	ctx                context.Context
	responses          []*csi.GetMetadataDeltaResponse
	failOutgoingAfterN int
}

func (m *mockDeltaStreamServer) Send(resp *csi.GetMetadataDeltaResponse) error {
	if m.failOutgoingAfterN > 0 && len(m.responses) >= m.failOutgoingAfterN {
		return fmt.Errorf("injected downstream send failure")
	}
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockDeltaStreamServer) Context() context.Context {
	return m.ctx
}

func setupMockSupervisorServer(t *testing.T) (*grpc.Server, *mockSupervisorSnapshotMetadataServer, string) {
	return setupMockSupervisorServerWithOptions(t, nil)
}

// installFakeTokenMinterForTest swaps the package-level
// mintSnapshotMetadataToken function with one that returns the supplied
// (token, err) on every invocation, ignoring its inputs. The original is
// restored via t.Cleanup. Production replaces this seam with a call to
// the Kubernetes TokenRequest API against the Supervisor — see
// defaultMintSnapshotMetadataToken in controller.go. Tests use it to:
//
//   - return a constant "test-token" matching the value the in-process
//     mock SMS server expects in the Authorization header;
//   - return a different token to exercise the Unauthenticated path;
//   - return an error to exercise the minter-failure path.
func installFakeTokenMinterForTest(t *testing.T, token string, mintErr error) {
	t.Helper()
	orig := mintSnapshotMetadataToken
	mintSnapshotMetadataToken = func(
		_ context.Context,
		_ clientset.Interface,
		_ string,
		_ string,
	) (string, error) {
		return token, mintErr
	}
	t.Cleanup(func() { mintSnapshotMetadataToken = orig })
}

// installInsecureDialForTest swaps the package-level dialSnapshotMetadata
// function with one that dials the in-process mock server using insecure
// (non-TLS) credentials, ignoring the production transport credentials
// passed by getSnapshotMetadataClient. The original is restored via
// t.Cleanup. Tests still need the SnapshotMetadataService CR to carry a
// valid caCert PEM so the production credential-construction path runs end
// to end (use testLocalhostTLSCert to obtain a PEM).
func installInsecureDialForTest(t *testing.T) {
	t.Helper()
	orig := dialSnapshotMetadata
	dialSnapshotMetadata = func(
		addr string,
		_ credentials.TransportCredentials,
		perRPC credentials.PerRPCCredentials,
	) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(perRPC),
		)
	}
	t.Cleanup(func() { dialSnapshotMetadata = orig })
}

// withProductionTLSDialForSubtest temporarily restores the production
// dialSnapshotMetadata for a single sub-test. It is intended for sub-tests
// that exercise the real TLS handshake against an in-process TLS server,
// inside a parent test that installed installInsecureDialForTest. The
// original (insecure) override is restored via t.Cleanup so unrelated
// sub-tests are unaffected.
func withProductionTLSDialForSubtest(t *testing.T) {
	t.Helper()
	orig := dialSnapshotMetadata
	dialSnapshotMetadata = func(
		addr string,
		creds credentials.TransportCredentials,
		perRPC credentials.PerRPCCredentials,
	) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr,
			grpc.WithTransportCredentials(creds),
			grpc.WithPerRPCCredentials(perRPC),
		)
	}
	t.Cleanup(func() { dialSnapshotMetadata = orig })
}

// setupBareGRPCServer listens with no SnapshotMetadata service registered (client RPC fails fast).
func setupBareGRPCServer(t *testing.T) (*grpc.Server, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("grpc server error: %v", err)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	return grpcServer, lis.Addr().String()
}

func setupMockSupervisorServerWithOptions(
	t *testing.T, tlsConf *tls.Config,
) (*grpc.Server, *mockSupervisorSnapshotMetadataServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var opts []grpc.ServerOption
	if tlsConf != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConf)))
	}
	grpcServer := grpc.NewServer(opts...)
	mockServer := &mockSupervisorSnapshotMetadataServer{}
	snapshotmetadataapi.RegisterSnapshotMetadataServer(grpcServer, mockServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("grpc server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	return grpcServer, mockServer, lis.Addr().String()
}

// testLocalhostTLSCert returns CA PEM (trust for client) and a tls.Certificate for a gRPC server on 127.0.0.1.
func testLocalhostTLSCert(t *testing.T) (caPEM []byte, serverCert tls.Certificate) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	servKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	servTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	servDER, err := x509.CreateCertificate(rand.Reader, servTmpl, caTmpl, &servKey.PublicKey, caKey)
	require.NoError(t, err)
	keyBytes, err := x509.MarshalPKCS8PrivateKey(servKey)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: servDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	serverCert, err = tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	return caPEM, serverCert
}

func TestGetMetadataAllocated(t *testing.T) {
	ct := getControllerTest(t)

	// Seed fake Supervisor VolumeSnapshotContents so the CSI handle -> VS namespace/name
	// translation done by the driver resolves for every handle used in the sub-tests.
	seedSupervisorVolumeSnapshots(t, ct.controller, "snap-1", "snap-invalid", "snap-2")

	// Set up mock supervisor server
	grpcServer, mockServer, addr := setupMockSupervisorServer(t)
	defer grpcServer.Stop()

	// Generate a valid CA PEM so the production credential-construction
	// path in getSnapshotMetadataClient succeeds. The actual TLS handshake
	// is bypassed by installInsecureDialForTest below, which swaps
	// dialSnapshotMetadata for an insecure dialer.
	caPEM, _ := testLocalhostTLSCert(t)
	smsCR := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cbt.storage.k8s.io/v1beta1",
			"kind":       "SnapshotMetadataService",
			"metadata": map[string]interface{}{
				"name": "csi.vsphere.vmware.com",
			},
			"spec": map[string]interface{}{
				"address":  addr,
				"caCert":   string(caPEM),
				"audience": "test-audience",
			},
		},
	}

	scheme := runtime.NewScheme()
	ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(smsCR).Build()

	// Install test-only seams: a fake token minter that returns the
	// constant "test-token" the mock SMS server checks for in the
	// Authorization header, and an insecure dialer (the production
	// binary always uses TLS — see dialSnapshotMetadata in
	// controller.go).
	installFakeTokenMinterForTest(t, "test-token", nil)
	installInsecureDialForTest(t)

	// Enable CBT FSS
	ctx := context.Background()
	if co, ok := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator); ok {
		_ = co.EnableFSS(ctx, common.CSI_Backup_API_FSS)
	}

	t.Run("successful streaming", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{
			{
				BlockMetadata: []*snapshotmetadataapi.BlockMetadata{
					{ByteOffset: 0, SizeBytes: 4096},
				},
				VolumeCapacityBytes: 8192,
			},
			{
				BlockMetadata: []*snapshotmetadataapi.BlockMetadata{
					{ByteOffset: 4096, SizeBytes: 4096},
				},
				VolumeCapacityBytes: 8192,
			},
		}

		req := &csi.GetMetadataAllocatedRequest{
			SnapshotId:     "snap-1",
			StartingOffset: 0,
			MaxResults:     10,
		}

		mockStream := &mockAllocatedStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(mockStream.responses))
		assert.Equal(t, int64(0), mockStream.responses[0].BlockMetadata[0].ByteOffset)
		assert.Equal(t, int64(4096), mockStream.responses[1].BlockMetadata[0].ByteOffset)
	})

	t.Run("missing snapshot id", func(t *testing.T) {
		req := &csi.GetMetadataAllocatedRequest{
			StartingOffset: 0,
			MaxResults:     10,
		}

		mockStream := &mockAllocatedStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("supervisor server error", func(t *testing.T) {
		mockServer.errToReturn = status.Error(codes.NotFound, "snapshot not found")

		req := &csi.GetMetadataAllocatedRequest{
			SnapshotId:     "snap-invalid",
			StartingOffset: 0,
			MaxResults:     10,
		}

		mockStream := &mockAllocatedStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("minted token rejected by supervisor", func(t *testing.T) {
		// Mint a token whose value differs from the one the mock SMS
		// server expects in the Authorization header. This exercises
		// the path where the audience-bound TokenRequest succeeds but
		// the supervisor rejects the call (e.g. audience mismatch on
		// the wire).
		mockServer.errToReturn = nil
		installFakeTokenMinterForTest(t, "wrong-token", nil)

		req := &csi.GetMetadataAllocatedRequest{
			SnapshotId:     "snap-1",
			StartingOffset: 0,
			MaxResults:     10,
		}

		mockStream := &mockAllocatedStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("token mint failure", func(t *testing.T) {
		// Simulate the Supervisor TokenRequest API rejecting our mint
		// (e.g. RBAC missing on the pvcsi-provider SA, or the API
		// being unreachable). The driver must surface this as an
		// Internal error rather than papering it over.
		mockServer.errToReturn = nil
		installFakeTokenMinterForTest(t, "", fmt.Errorf("simulated TokenRequest failure"))

		req := &csi.GetMetadataAllocatedRequest{
			SnapshotId:     "snap-1",
			StartingOffset: 0,
			MaxResults:     10,
		}

		mockStream := &mockAllocatedStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "audience-bound Supervisor token")
	})

	t.Run("nil request", func(t *testing.T) {
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(nil, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("empty supervisor stream EOF", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.allocatedResponses = nil
		mockServer.afterSendsReturn = nil
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.NoError(t, err)
		assert.Empty(t, mockStream.responses)
	})

	t.Run("supervisor GetMetadataAllocated RPC not implemented", func(t *testing.T) {
		prevClient := ct.controller.cnsOperatorClient
		defer func() { ct.controller.cnsOperatorClient = prevClient }()

		bareSrv, addr := setupBareGRPCServer(t)
		defer bareSrv.Stop()

		bareCAPEM, _ := testLocalhostTLSCert(t)
		smsCR := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  addr,
					"caCert":   string(bareCAPEM),
					"audience": "test-audience",
				},
			},
		}
		ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().
			WithScheme(runtime.NewScheme()).WithObjects(smsCR).Build()

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.NotEqual(t, codes.OK, status.Code(err))
	})

	t.Run("OutOfRange from supervisor ends stream", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{{VolumeCapacityBytes: 100}}
		mockServer.afterSendsReturn = status.Error(codes.OutOfRange, "end")
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.NoError(t, err)
		assert.Len(t, mockStream.responses, 1)
	})

	t.Run("Recv error from supervisor", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{{VolumeCapacityBytes: 100}}
		mockServer.afterSendsReturn = status.Error(codes.ResourceExhausted, "slow down")
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("DeadlineExceeded from supervisor", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{{VolumeCapacityBytes: 100}}
		mockServer.afterSendsReturn = status.Error(codes.DeadlineExceeded, "timeout")
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	})

	t.Run("downstream Send failure", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.afterSendsReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{
			{VolumeCapacityBytes: 100},
			{VolumeCapacityBytes: 200},
		}

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx, failOutgoingAfterN: 1}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.Error(t, err)
		assert.Len(t, mockStream.responses, 1)
	})

	t.Run("supervisor stream canceled when client context canceled", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.afterSendsReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{{VolumeCapacityBytes: 100}}
		mockServer.blockAfterSendsUntilCtxDone = true

		ctx2, cancel := context.WithCancel(ctx)
		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx2}
		errCh := make(chan error, 1)
		go func() { errCh <- ct.controller.GetMetadataAllocated(req, mockStream) }()
		time.Sleep(300 * time.Millisecond)
		cancel()
		select {
		case err := <-errCh:
			assert.Error(t, err)
			assert.Equal(t, codes.Canceled, status.Code(err))
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for GetMetadataAllocated")
		}
		mockServer.blockAfterSendsUntilCtxDone = false
	})

	t.Run("tls caCert PEM to supervisor", func(t *testing.T) {
		prevClient := ct.controller.cnsOperatorClient
		defer func() { ct.controller.cnsOperatorClient = prevClient }()
		withProductionTLSDialForSubtest(t)

		caPEM, leaf := testLocalhostTLSCert(t)
		tlsConf := &tls.Config{Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12}
		grpcServer, mockServer, addr := setupMockSupervisorServerWithOptions(t, tlsConf)
		defer grpcServer.Stop()

		smsCR := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  addr,
					"caCert":   string(caPEM),
					"audience": "test-audience",
				},
			},
		}
		ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().
			WithScheme(runtime.NewScheme()).WithObjects(smsCR).Build()

		mockServer.errToReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{{VolumeCapacityBytes: 500}}

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.NoError(t, err)
		assert.Len(t, mockStream.responses, 1)
	})

	t.Run("tls caCert base64-encoded PEM", func(t *testing.T) {
		prevClient := ct.controller.cnsOperatorClient
		defer func() { ct.controller.cnsOperatorClient = prevClient }()
		withProductionTLSDialForSubtest(t)

		caPEM, leaf := testLocalhostTLSCert(t)
		tlsConf := &tls.Config{Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12}
		grpcServer, mockServer, addr := setupMockSupervisorServerWithOptions(t, tlsConf)
		defer grpcServer.Stop()

		smsCR := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  addr,
					"caCert":   base64.StdEncoding.EncodeToString(caPEM),
					"audience": "test-audience",
				},
			},
		}
		ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().
			WithScheme(runtime.NewScheme()).WithObjects(smsCR).Build()

		mockServer.errToReturn = nil
		mockServer.allocatedResponses = []*snapshotmetadataapi.GetMetadataAllocatedResponse{{VolumeCapacityBytes: 500}}

		req := &csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10}
		mockStream := &mockAllocatedStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataAllocated(req, mockStream)
		assert.NoError(t, err)
		assert.Len(t, mockStream.responses, 1)
	})
}

func TestGetMetadataDelta(t *testing.T) {
	ct := getControllerTest(t)

	// Seed fake Supervisor VolumeSnapshotContents so the CSI target handle -> VS name
	// translation resolves for every handle used in the sub-tests.
	seedSupervisorVolumeSnapshots(t, ct.controller, "snap-1", "snap-2", "snap-invalid")

	// Set up mock supervisor server
	grpcServer, mockServer, addr := setupMockSupervisorServer(t)
	defer grpcServer.Stop()

	// Generate a valid CA PEM so the production credential-construction
	// path in getSnapshotMetadataClient succeeds. The actual TLS handshake
	// is bypassed by installInsecureDialForTest below, which swaps
	// dialSnapshotMetadata for an insecure dialer.
	caPEM, _ := testLocalhostTLSCert(t)
	smsCR := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cbt.storage.k8s.io/v1beta1",
			"kind":       "SnapshotMetadataService",
			"metadata": map[string]interface{}{
				"name": "csi.vsphere.vmware.com",
			},
			"spec": map[string]interface{}{
				"address":  addr,
				"caCert":   string(caPEM),
				"audience": "test-audience",
			},
		},
	}

	scheme := runtime.NewScheme()
	ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(smsCR).Build()

	// Install test-only seams: a fake token minter that returns the
	// constant "test-token" the mock SMS server checks for in the
	// Authorization header, and an insecure dialer (the production
	// binary always uses TLS — see dialSnapshotMetadata in
	// controller.go).
	installFakeTokenMinterForTest(t, "test-token", nil)
	installInsecureDialForTest(t)

	// Enable CBT FSS
	ctx := context.Background()
	if co, ok := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator); ok {
		_ = co.EnableFSS(ctx, common.CSI_Backup_API_FSS)
	}

	t.Run("successful streaming", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.deltaResponses = []*snapshotmetadataapi.GetMetadataDeltaResponse{
			{
				BlockMetadata: []*snapshotmetadataapi.BlockMetadata{
					{ByteOffset: 0, SizeBytes: 4096},
				},
				VolumeCapacityBytes: 16384,
			},
		}

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId:   "snap-1",
			TargetSnapshotId: "snap-2",
			StartingOffset:   0,
			MaxResults:       10,
		}

		mockStream := &mockDeltaStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(mockStream.responses))
		assert.Equal(t, int64(0), mockStream.responses[0].BlockMetadata[0].ByteOffset)
		// Verify the pvCSI driver:
		//   - forwarded BaseSnapshotId verbatim (it is the vSphere change-id; backup software
		//     supplies it directly and the driver MUST NOT mutate it, look it up, or translate
		//     it via cluster-scoped VolumeSnapshotContent on the Supervisor),
		//   - translated only the target CSI handle to the Supervisor VolumeSnapshot
		//     (namespace, name) using a namespace-scoped Get.
		require.NotNil(t, mockServer.lastDeltaReq)
		assert.Equal(t, "snap-1", mockServer.lastDeltaReq.BaseSnapshotId)
		assert.Equal(t, "snap-2", mockServer.lastDeltaReq.TargetSnapshotName)
		assert.Equal(t, testSupervisorSnapshotNamespace, mockServer.lastDeltaReq.Namespace)
	})

	t.Run("missing base snapshot id", func(t *testing.T) {
		req := &csi.GetMetadataDeltaRequest{
			TargetSnapshotId: "snap-2",
			StartingOffset:   0,
			MaxResults:       10,
		}

		mockStream := &mockDeltaStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("missing target snapshot id", func(t *testing.T) {
		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1",
			StartingOffset: 0,
			MaxResults:     10,
		}

		mockStream := &mockDeltaStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("supervisor server error", func(t *testing.T) {
		mockServer.errToReturn = status.Error(codes.Internal, "internal error")

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId:   "snap-1",
			TargetSnapshotId: "snap-invalid",
			StartingOffset:   0,
			MaxResults:       10,
		}

		mockStream := &mockDeltaStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("minted token rejected by supervisor", func(t *testing.T) {
		mockServer.errToReturn = nil
		installFakeTokenMinterForTest(t, "wrong-token", nil)

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId:   "snap-1",
			TargetSnapshotId: "snap-2",
			StartingOffset:   0,
			MaxResults:       10,
		}

		mockStream := &mockDeltaStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("token mint failure", func(t *testing.T) {
		mockServer.errToReturn = nil
		installFakeTokenMinterForTest(t, "", fmt.Errorf("simulated TokenRequest failure"))

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId:   "snap-1",
			TargetSnapshotId: "snap-2",
			StartingOffset:   0,
			MaxResults:       10,
		}

		mockStream := &mockDeltaStreamServer{
			ctx: ctx,
		}

		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "audience-bound Supervisor token")
	})

	t.Run("nil request", func(t *testing.T) {
		mockStream := &mockDeltaStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataDelta(nil, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("empty supervisor stream EOF", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.deltaResponses = nil
		mockServer.afterSendsReturn = nil
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1", TargetSnapshotId: "snap-2", StartingOffset: 0, MaxResults: 10,
		}
		mockStream := &mockDeltaStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.NoError(t, err)
		assert.Empty(t, mockStream.responses)
	})

	t.Run("supervisor GetMetadataDelta RPC not implemented", func(t *testing.T) {
		prevClient := ct.controller.cnsOperatorClient
		defer func() { ct.controller.cnsOperatorClient = prevClient }()

		bareSrv, addr := setupBareGRPCServer(t)
		defer bareSrv.Stop()

		bareCAPEM, _ := testLocalhostTLSCert(t)
		smsCR := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  addr,
					"caCert":   string(bareCAPEM),
					"audience": "test-audience",
				},
			},
		}
		ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().
			WithScheme(runtime.NewScheme()).WithObjects(smsCR).Build()

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1", TargetSnapshotId: "snap-2", StartingOffset: 0, MaxResults: 10,
		}
		mockStream := &mockDeltaStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.NotEqual(t, codes.OK, status.Code(err))
	})

	t.Run("OutOfRange from supervisor ends stream", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.deltaResponses = []*snapshotmetadataapi.GetMetadataDeltaResponse{{VolumeCapacityBytes: 100}}
		mockServer.afterSendsReturn = status.Error(codes.OutOfRange, "end")
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1", TargetSnapshotId: "snap-2", StartingOffset: 0, MaxResults: 10,
		}
		mockStream := &mockDeltaStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.NoError(t, err)
		assert.Len(t, mockStream.responses, 1)
	})

	t.Run("Recv error from supervisor", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.deltaResponses = []*snapshotmetadataapi.GetMetadataDeltaResponse{{VolumeCapacityBytes: 100}}
		mockServer.afterSendsReturn = status.Error(codes.ResourceExhausted, "slow down")
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1", TargetSnapshotId: "snap-2", StartingOffset: 0, MaxResults: 10,
		}
		mockStream := &mockDeltaStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("DeadlineExceeded from supervisor", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.deltaResponses = []*snapshotmetadataapi.GetMetadataDeltaResponse{{VolumeCapacityBytes: 100}}
		mockServer.afterSendsReturn = status.Error(codes.DeadlineExceeded, "timeout")
		mockServer.blockAfterSendsUntilCtxDone = false

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1", TargetSnapshotId: "snap-2", StartingOffset: 0, MaxResults: 10,
		}
		mockStream := &mockDeltaStreamServer{ctx: ctx}
		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	})

	t.Run("downstream Send failure", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.afterSendsReturn = nil
		mockServer.deltaResponses = []*snapshotmetadataapi.GetMetadataDeltaResponse{
			{VolumeCapacityBytes: 100},
			{VolumeCapacityBytes: 200},
		}

		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1", TargetSnapshotId: "snap-2", StartingOffset: 0, MaxResults: 10,
		}
		mockStream := &mockDeltaStreamServer{ctx: ctx, failOutgoingAfterN: 1}
		err := ct.controller.GetMetadataDelta(req, mockStream)
		assert.Error(t, err)
		assert.Len(t, mockStream.responses, 1)
	})

	t.Run("supervisor stream canceled when client context canceled", func(t *testing.T) {
		mockServer.errToReturn = nil
		mockServer.afterSendsReturn = nil
		mockServer.deltaResponses = []*snapshotmetadataapi.GetMetadataDeltaResponse{{VolumeCapacityBytes: 100}}
		mockServer.blockAfterSendsUntilCtxDone = true

		ctx2, cancel := context.WithCancel(ctx)
		req := &csi.GetMetadataDeltaRequest{
			BaseSnapshotId: "snap-1", TargetSnapshotId: "snap-2", StartingOffset: 0, MaxResults: 10,
		}
		mockStream := &mockDeltaStreamServer{ctx: ctx2}
		errCh := make(chan error, 1)
		go func() { errCh <- ct.controller.GetMetadataDelta(req, mockStream) }()
		time.Sleep(300 * time.Millisecond)
		cancel()
		select {
		case err := <-errCh:
			assert.Error(t, err)
			assert.Equal(t, codes.Canceled, status.Code(err))
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for GetMetadataDelta")
		}
		mockServer.blockAfterSendsUntilCtxDone = false
	})
}

func TestGetMetadataAllocated_CBTFSSDisabled(t *testing.T) {
	ct := getControllerTest(t)
	ctx := context.Background()
	if co, ok := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator); ok {
		require.NoError(t, co.DisableFSS(ctx, common.CSI_Backup_API_FSS))
		t.Cleanup(func() { _ = co.EnableFSS(ctx, common.CSI_Backup_API_FSS) })
	}

	mockStream := &mockAllocatedStreamServer{ctx: ctx}
	err := ct.controller.GetMetadataAllocated(&csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1"}, mockStream)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestGetMetadataDelta_CBTFSSDisabled(t *testing.T) {
	ct := getControllerTest(t)
	ctx := context.Background()
	if co, ok := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator); ok {
		require.NoError(t, co.DisableFSS(ctx, common.CSI_Backup_API_FSS))
		t.Cleanup(func() { _ = co.EnableFSS(ctx, common.CSI_Backup_API_FSS) })
	}

	mockStream := &mockDeltaStreamServer{ctx: ctx}
	err := ct.controller.GetMetadataDelta(&csi.GetMetadataDeltaRequest{
		BaseSnapshotId: "a", TargetSnapshotId: "b",
	}, mockStream)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestGetSnapshotMetadataClient_Errors(t *testing.T) {
	ct := getControllerTest(t)
	ctx := context.Background()
	if co, ok := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator); ok {
		require.NoError(t, co.EnableFSS(ctx, common.CSI_Backup_API_FSS))
	}
	// All sub-tests here exercise failures that occur after the CSI handle -> Supervisor VS
	// lookup, so the lookup itself must succeed for "snap-1".
	seedSupervisorVolumeSnapshots(t, ct.controller, "snap-1")

	// Install a test-only token minter (the production binary always
	// uses TokenRequest — see defaultMintSnapshotMetadataToken in
	// controller.go) and an insecure dialer for the whole function.
	// Sub-tests that assert errors before the dial path is reached
	// (invalid PEM, empty caCert, missing audience, etc.) never invoke
	// these; the "grpc dial failure" sub-test relies on the dialer to
	// hit 127.0.0.1:1 and return Unavailable.
	installFakeTokenMinterForTest(t, "test-token", nil)
	installInsecureDialForTest(t)

	runAllocated := func(sms *unstructured.Unstructured) error {
		ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().
			WithScheme(runtime.NewScheme()).WithObjects(sms).Build()
		return ct.controller.GetMetadataAllocated(
			&csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10},
			&mockAllocatedStreamServer{ctx: ctx},
		)
	}

	t.Run("SnapshotMetadataService CR missing", func(t *testing.T) {
		ct.controller.cnsOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
		err := ct.controller.GetMetadataAllocated(
			&csi.GetMetadataAllocatedRequest{SnapshotId: "snap-1", StartingOffset: 0, MaxResults: 10},
			&mockAllocatedStreamServer{ctx: ctx},
		)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "failed to get SnapshotMetadataService CR")
	})

	t.Run("empty spec.address", func(t *testing.T) {
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec":       map[string]interface{}{"address": "", "caCert": ""},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "address")
	})

	t.Run("missing spec.audience", func(t *testing.T) {
		// spec.audience is mandatory: the Supervisor SMS sidecar uses
		// it to bind the TokenReview audience, so an empty audience
		// would silently fall back to the API server default audience
		// (the fragile pre-TokenRequest behaviour we deliberately
		// replaced). The driver must reject this loudly.
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec":       map[string]interface{}{"address": "127.0.0.1:9", "audience": ""},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "audience")
	})

	t.Run("missing spec.caCert", func(t *testing.T) {
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec":       map[string]interface{}{"address": "127.0.0.1:9", "audience": "test-audience"},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "caCert")
	})

	t.Run("invalid caCert PEM and base64", func(t *testing.T) {
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  "127.0.0.1:9",
					"audience": "test-audience",
					"caCert":   "not-valid-pem",
				},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "caCert")
	})

	t.Run("base64 decodes but PEM append still fails", func(t *testing.T) {
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  "127.0.0.1:9",
					"audience": "test-audience",
					"caCert":   base64.StdEncoding.EncodeToString([]byte("not a pem block")),
				},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("empty caCert is rejected", func(t *testing.T) {
		// TLS is mandatory for the Supervisor dial — an empty spec.caCert
		// must always be rejected (the production binary has no insecure
		// fallback; see getSnapshotMetadataClient in controller.go).
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  "127.0.0.1:9",
					"audience": "test-audience",
					"caCert":   "",
				},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "caCert is empty")
	})

	t.Run("token mint failure", func(t *testing.T) {
		// Verify that a TokenRequest API failure (e.g. RBAC missing or
		// API server unreachable) is surfaced as Internal. We override
		// the minter just for this sub-test; t.Cleanup restores the
		// happy-path minter installed at the top of the test function.
		caPEM, _ := testLocalhostTLSCert(t)
		installFakeTokenMinterForTest(t, "", fmt.Errorf("simulated TokenRequest failure"))
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  "127.0.0.1:1",
					"audience": "test-audience",
					"caCert":   string(caPEM),
				},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), "audience-bound Supervisor token")
	})

	t.Run("grpc dial failure", func(t *testing.T) {
		// Provide a valid CA PEM so the production credential-construction
		// path passes; the dial itself goes through the test override
		// installed at the top of TestGetSnapshotMetadataClient_Errors and
		// then fails because nothing is listening on 127.0.0.1:1.
		caPEM, _ := testLocalhostTLSCert(t)
		sms := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "cbt.storage.k8s.io/v1beta1",
				"kind":       "SnapshotMetadataService",
				"metadata":   map[string]interface{}{"name": "csi.vsphere.vmware.com"},
				"spec": map[string]interface{}{
					"address":  "127.0.0.1:1",
					"audience": "test-audience",
					"caCert":   string(caPEM),
				},
			},
		}
		err := runAllocated(sms)
		require.Error(t, err)
		// gRPC surfaces connection refused as Unavailable when the client fails the RPC (lazy dial).
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})
}
