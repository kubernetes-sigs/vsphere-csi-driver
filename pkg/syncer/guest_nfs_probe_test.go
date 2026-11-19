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

package syncer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// rpcRecord returns words as one record-marked RPC message.
func rpcRecord(words ...uint32) []byte {
	b := make([]byte, 4, 4+4*len(words))
	for _, w := range words {
		b = binary.BigEndian.AppendUint32(b, w)
	}
	binary.BigEndian.PutUint32(b, rpcLastFragment|uint32(len(b)-4))
	return b
}

// acceptedReply returns an accepted RPC reply with an AUTH_NONE verifier.
func acceptedReply(xid, acceptStat uint32, extra ...uint32) []byte {
	return rpcRecord(append([]uint32{xid, rpcMsgReply, rpcMsgAccepted, rpcAuthNone, 0, acceptStat}, extra...)...)
}

// fakeNFSServer is an in-process TCP server that hands each connection to serve.
type fakeNFSServer struct {
	listener net.Listener
	serve    func(s *fakeNFSServer, conn net.Conn)
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	calls    atomic.Int32
}

func startFakeNFSServer(t *testing.T, address string, serve func(s *fakeNFSServer, conn net.Conn)) *fakeNFSServer {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	s := &fakeNFSServer{listener: listener, serve: serve, conns: map[net.Conn]struct{}{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer conn.Close()
				s.serve(s, conn)
			}()
		}
	}()
	t.Cleanup(s.stop)
	return s
}

func (s *fakeNFSServer) address() string { return s.listener.Addr().String() }

func (s *fakeNFSServer) port() int { return s.listener.Addr().(*net.TCPAddr).Port }

// stop closes the listener and every open connection, and waits for all handlers to return.
func (s *fakeNFSServer) stop() {
	_ = s.listener.Close()
	s.mu.Lock()
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// answerNullCalls returns a handler that reads one call, checks that it is the NFS v4 NULL call, and writes
// reply(xid) back, or closes the connection when reply returns nil.
func answerNullCalls(t *testing.T, reply func(xid uint32) []byte) func(*fakeNFSServer, net.Conn) {
	return func(s *fakeNFSServer, conn net.Conn) {
		call, err := readRPCRecord(conn)
		if err != nil {
			return
		}
		s.calls.Add(1)
		r := xdrReader{buf: call}
		xid := r.uint32()
		var body []uint32
		for !r.short {
			if w := r.uint32(); !r.short {
				body = append(body, w)
			}
		}
		assert.Equal(t, []uint32{rpcMsgCall, rpcVersion, rpcProgramNFS, rpcNFSVersion, rpcProcNull,
			rpcAuthNone, 0, rpcAuthNone, 0}, body, "not an NFS v4 NULL call with AUTH_NONE")
		if b := reply(xid); b != nil {
			_, _ = conn.Write(b)
		}
	}
}

// nfsServerReplying starts a fake NFS server on 127.0.0.1 that answers every NULL call with reply.
func nfsServerReplying(t *testing.T, reply func(xid uint32) []byte) *fakeNFSServer {
	t.Helper()
	return startFakeNFSServer(t, "127.0.0.1:0", answerNullCalls(t, reply))
}

func successReply(xid uint32) []byte { return acceptedReply(xid, 0) }

// closedPortAddress returns a 127.0.0.1 address on which nothing listens.
func closedPortAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func TestNFSNullCallEncoding(t *testing.T) {
	want := []byte{
		0x80, 0, 0, 40, // record mark: last fragment, 40 bytes
		0x12, 0x34, 0x56, 0x78, // xid
		0, 0, 0, 0, // CALL
		0, 0, 0, 2, // RPC version 2
		0, 0x01, 0x86, 0xa3, // program 100003 (NFS)
		0, 0, 0, 4, // version 4
		0, 0, 0, 0, // procedure 0 (NULL)
		0, 0, 0, 0, 0, 0, 0, 0, // credentials: AUTH_NONE, empty
		0, 0, 0, 0, 0, 0, 0, 0, // verifier: AUTH_NONE, empty
	}
	assert.Equal(t, want, nfsNullCall(0x12345678))
}

func TestCheckNFSNullReply(t *testing.T) {
	const xid = 7
	strip := func(record []byte) []byte { return record[4:] }
	for name, tc := range map[string]struct {
		reply   []byte
		wantErr error
	}{
		"success":                    {strip(acceptedReply(xid, 0)), nil},
		"program version mismatch":   {strip(acceptedReply(xid, 2, 3, 3)), nil},
		"procedure unavailable":      {strip(acceptedReply(xid, 3)), nil},
		"garbage arguments":          {strip(acceptedReply(xid, 4)), nil},
		"system error":               {strip(acceptedReply(xid, 5)), nil},
		"denied, RPC version":        {strip(rpcRecord(xid, rpcMsgReply, rpcMsgDenied, 0, 2, 2)), nil},
		"denied, authentication":     {strip(rpcRecord(xid, rpcMsgReply, rpcMsgDenied, 1, 1)), nil},
		"verifier with a body":       {strip(rpcRecord(xid, rpcMsgReply, rpcMsgAccepted, 1, 5, 1, 2, 0)), nil},
		"program unavailable":        {strip(acceptedReply(xid, 1)), errNFSProgramUnavailable},
		"other xid":                  {strip(acceptedReply(xid+1, 0)), errNotRPCReply},
		"a call, not a reply":        {strip(rpcRecord(xid, rpcMsgCall, 2, rpcProgramNFS)), errNotRPCReply},
		"unknown reply status":       {strip(rpcRecord(xid, rpcMsgReply, 2, 0)), errNotRPCReply},
		"unknown accept status":      {strip(acceptedReply(xid, 6)), errNotRPCReply},
		"unknown reject status":      {strip(rpcRecord(xid, rpcMsgReply, rpcMsgDenied, 2)), errNotRPCReply},
		"no accept status":           {strip(rpcRecord(xid, rpcMsgReply, rpcMsgAccepted, 0, 0)), errNotRPCReply},
		"verifier longer than reply": {strip(rpcRecord(xid, rpcMsgReply, rpcMsgAccepted, 1, 400, 0)), errNotRPCReply},
		"no reject status":           {strip(rpcRecord(xid, rpcMsgReply, rpcMsgDenied)), errNotRPCReply},
		"truncated header":           {[]byte{0, 0, 0, xid, 0, 0}, errNotRPCReply},
		"empty":                      {nil, errNotRPCReply},
		"text":                       {[]byte("HTTP/1.1 400 Bad Request\r\n\r\n"), errNotRPCReply},
	} {
		t.Run(name, func(t *testing.T) {
			err := checkNFSNullReply(tc.reply, xid)
			if tc.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.wantErr)
			}
		})
	}
	assert.ErrorIs(t, checkNFSNullReply(nil, 0), errNotRPCReply, "an empty reply must not match xid 0")
}

func TestReadRPCRecord(t *testing.T) {
	fragments := func(parts ...[]byte) []byte {
		var b []byte
		for i, p := range parts {
			mark := uint32(len(p))
			if i == len(parts)-1 {
				mark |= rpcLastFragment
			}
			b = binary.BigEndian.AppendUint32(b, mark)
			b = append(b, p...)
		}
		return b
	}
	got, err := readRPCRecord(bytes.NewReader(fragments([]byte("abcd"), nil, []byte("efgh"))))
	require.NoError(t, err)
	assert.Equal(t, []byte("abcdefgh"), got, "fragments are joined")

	_, err = readRPCRecord(bytes.NewReader(fragments(make([]byte, rpcMaxReplySize+1))))
	assert.ErrorIs(t, err, errNotRPCReply, "an oversized record is not read")
	_, err = readRPCRecord(bytes.NewReader(fragments(make([]byte, rpcMaxReplySize), []byte("x"))))
	assert.ErrorIs(t, err, errNotRPCReply, "the size limit covers all fragments")
	_, err = readRPCRecord(bytes.NewReader([]byte("HTTP/1.1 400 Bad Request\r\n\r\n")))
	assert.ErrorIs(t, err, errNotRPCReply, "text is read as an oversized record mark")
	_, err = readRPCRecord(bytes.NewReader(fragments([]byte("abcd"))[:6]))
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF, "truncated fragment")
	_, err = readRPCRecord(bytes.NewReader(nil))
	assert.ErrorIs(t, err, io.EOF)
}

func TestProbeNFSServiceAnswers(t *testing.T) {
	for name, reply := range map[string]func(xid uint32) []byte{
		"success":                  successReply,
		"program version mismatch": func(xid uint32) []byte { return acceptedReply(xid, 2, 3, 3) },
		"authentication denied":    func(xid uint32) []byte { return rpcRecord(xid, rpcMsgReply, rpcMsgDenied, 1, 1) },
		"reply in two fragments": func(xid uint32) []byte {
			full := successReply(xid)[4:]
			first := binary.BigEndian.AppendUint32(nil, 8)
			first = append(first, full[:8]...)
			last := binary.BigEndian.AppendUint32(nil, rpcLastFragment|uint32(len(full)-8))
			return append(append(first, last...), full[8:]...)
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := nfsServerReplying(t, reply)
			require.NoError(t, probeNFSService(context.Background(), server.address(), 5*time.Second))
			assert.EqualValues(t, 1, server.calls.Load())
		})
	}
}

func TestProbeNFSServiceFailures(t *testing.T) {
	ctx := context.Background()
	t.Run("not an NFS program", func(t *testing.T) {
		server := nfsServerReplying(t, func(xid uint32) []byte { return acceptedReply(xid, 1) })
		assert.ErrorIs(t, probeNFSService(ctx, server.address(), 5*time.Second), errNFSProgramUnavailable)
	})
	t.Run("reply to another call", func(t *testing.T) {
		server := nfsServerReplying(t, func(xid uint32) []byte { return successReply(xid + 1) })
		assert.ErrorIs(t, probeNFSService(ctx, server.address(), 5*time.Second), errNotRPCReply)
	})
	t.Run("garbage reply", func(t *testing.T) {
		server := nfsServerReplying(t, func(uint32) []byte { return []byte("HTTP/1.1 400 Bad Request\r\n\r\n") })
		assert.ErrorIs(t, probeNFSService(ctx, server.address(), 5*time.Second), errNotRPCReply)
	})
	t.Run("closed after the call", func(t *testing.T) {
		server := nfsServerReplying(t, func(uint32) []byte { return nil })
		assert.ErrorIs(t, probeNFSService(ctx, server.address(), 5*time.Second), io.EOF)
	})
	t.Run("closed on accept", func(t *testing.T) {
		server := startFakeNFSServer(t, "127.0.0.1:0", func(*fakeNFSServer, net.Conn) {})
		assert.Error(t, probeNFSService(ctx, server.address(), 5*time.Second))
	})
	t.Run("closed port", func(t *testing.T) {
		err := probeNFSService(ctx, closedPortAddress(t), 5*time.Second)
		var opErr *net.OpError
		require.ErrorAs(t, err, &opErr)
		assert.Equal(t, "dial", opErr.Op)
	})
	t.Run("name that does not resolve", func(t *testing.T) {
		err := probeNFSService(ctx, net.JoinHostPort("nfs-server.invalid", "2049"), 5*time.Second)
		require.Error(t, err)
		var dnsErr *net.DNSError
		var opErr *net.OpError
		assert.True(t, errors.As(err, &dnsErr) || errors.As(err, &opErr), "got %v", err)
	})
}

func TestProbeNFSServiceTimesOut(t *testing.T) {
	// The server reads the call and never answers; its cleanup closes the connections.
	server := startFakeNFSServer(t, "127.0.0.1:0", func(_ *fakeNFSServer, conn net.Conn) {
		_, _ = readRPCRecord(conn)
		_, _ = io.Copy(io.Discard, conn)
	})

	start := time.Now()
	err := probeNFSService(context.Background(), server.address(), 200*time.Millisecond)
	elapsed := time.Since(start)
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
	assert.Less(t, elapsed, 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start = time.Now()
	require.Error(t, probeNFSService(ctx, server.address(), time.Minute))
	assert.Less(t, time.Since(start), 5*time.Second, "cancelling ctx must end the probe before its timeout")
}

func guestNFSPVWithAttributes(attributes map[string]string, mountOptions ...string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-nfs"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{
				Driver:           common.VSphereCSIDriverName,
				VolumeHandle:     common.GuestNFSVolumeIDPrefix + "10.9.9.9#export#pvc-1#pvc-1#",
				VolumeAttributes: attributes,
			}},
			MountOptions: mountOptions,
		},
	}
}

func TestGuestNFSServerAddress(t *testing.T) {
	server := map[string]string{"server": "10.0.0.1", "share": "/export"}
	withContextOptions := func(key, options string) map[string]string {
		return map[string]string{"server": "10.0.0.1", key: options}
	}
	for name, tc := range map[string]struct {
		attributes   map[string]string
		mountOptions []string
		want         string
	}{
		"default port":          {server, nil, "10.0.0.1:2049"},
		"attribute key case":    {map[string]string{"Server": "nfs.example.com"}, nil, "nfs.example.com:2049"},
		"PV mount option":       {server, []string{"nfsvers=4.1", "port=2050"}, "10.0.0.1:2050"},
		"comma separated":       {server, []string{"nfsvers=4.1,port=2051,hard"}, "10.0.0.1:2051"},
		"surrounding spaces":    {server, []string{"hard, port=2053"}, "10.0.0.1:2053"},
		"last port wins":        {server, []string{"port=2050", "port=2054"}, "10.0.0.1:2054"},
		"volume attribute":      {withContextOptions("mountOptions", "port=2055"), nil, "10.0.0.1:2055"},
		"attribute key case 2":  {withContextOptions("MOUNTOPTIONS", "port=2056"), nil, "10.0.0.1:2056"},
		"attribute after PV":    {withContextOptions("mountOptions", "port=2057"), []string{"port=2050"}, "10.0.0.1:2057"},
		"mountport is not port": {server, []string{"mountport=20048"}, "10.0.0.1:2049"},
		"port 0 means default":  {server, []string{"port=0"}, "10.0.0.1:2049"},
		"IPv6":                  {map[string]string{"server": "fd00::1"}, []string{"port=2050"}, "[fd00::1]:2050"},
		"bracketed IPv6":        {map[string]string{"server": "[fd00::1]"}, nil, "[fd00::1]:2049"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := guestNFSServerAddress(guestNFSPVWithAttributes(tc.attributes, tc.mountOptions...))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
	for name, tc := range map[string]struct {
		attributes   map[string]string
		mountOptions []string
	}{
		"no server attribute": {map[string]string{"share": "/export"}, nil},
		"empty server":        {map[string]string{"server": ""}, nil},
		"port not a number":   {map[string]string{"server": "10.0.0.1"}, []string{"port=nfs"}},
		"port out of range":   {map[string]string{"server": "10.0.0.1"}, []string{"port=65536"}},
		"negative port":       {map[string]string{"server": "10.0.0.1", "mountOptions": "port=-1"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := guestNFSServerAddress(guestNFSPVWithAttributes(tc.attributes, tc.mountOptions...))
			assert.Error(t, err)
		})
	}

	t.Run("mount options are not modified", func(t *testing.T) {
		options := make([]string, 1, 4)
		options[0] = "port=2050"
		pv := guestNFSPVWithAttributes(map[string]string{"server": "10.0.0.1", "mountOptions": "port=2051"},
			options...)
		_, err := guestNFSServerAddress(pv)
		require.NoError(t, err)
		assert.Equal(t, []string{"port=2050"}, pv.Spec.MountOptions)
		assert.Equal(t, []string{"port=2050", ""}, options[:2], "the PV's backing array must not be appended to")
	})
}

// withGuestNFSProbe replaces guestNFSProbe for the duration of the test.
func withGuestNFSProbe(t *testing.T, probe func(ctx context.Context, address string) error) {
	t.Helper()
	orig := guestNFSProbe
	guestNFSProbe = probe
	t.Cleanup(func() { guestNFSProbe = orig })
}

func TestProbeGuestNFSServers(t *testing.T) {
	var mu sync.Mutex
	probed := map[string]int{}
	var active, maxActive atomic.Int32
	withGuestNFSProbe(t, func(ctx context.Context, address string) error {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		mu.Lock()
		probed[address]++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		if address == "down:2049" {
			return errors.New("connection refused")
		}
		return nil
	})

	addresses := []string{"down:2049"}
	for i := range 3 * guestNFSProbeWorkers {
		addresses = append(addresses, "nfs-"+strconv.Itoa(i)+":2049")
	}
	results := probeGuestNFSServers(context.Background(), addresses)

	require.Len(t, results, len(addresses))
	for _, address := range addresses {
		assert.Equal(t, 1, probed[address], "%s must be probed exactly once", address)
	}
	assert.EqualError(t, results["down:2049"], "connection refused")
	assert.NoError(t, results["nfs-0:2049"])
	assert.LessOrEqual(t, int(maxActive.Load()), guestNFSProbeWorkers, "concurrency is bounded")
	assert.Greater(t, maxActive.Load(), int32(1), "services are probed concurrently")
	assert.Zero(t, active.Load(), "no probe may still run after probeGuestNFSServers returns")

	assert.Empty(t, probeGuestNFSServers(context.Background(), nil))
}

// When ctx ends, probes cut short and probes never started have no result, and no probe outlives the call.
func TestProbeGuestNFSServersStopsWithContext(t *testing.T) {
	var active, started atomic.Int32
	withGuestNFSProbe(t, func(ctx context.Context, address string) error {
		active.Add(1)
		defer active.Add(-1)
		started.Add(1)
		if address == "fast:2049" {
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	})
	addresses := []string{"fast:2049"}
	for i := range 4 * guestNFSProbeWorkers {
		addresses = append(addresses, fmt.Sprintf("hung-%d:2049", i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results := probeGuestNFSServers(ctx, addresses)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, map[string]error{"fast:2049": nil}, results)
	assert.Zero(t, active.Load())
	assert.LessOrEqual(t, int(started.Load()), guestNFSProbeWorkers+1, "no new probe starts once ctx is done")
}
