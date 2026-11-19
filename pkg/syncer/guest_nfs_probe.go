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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
)

const (
	// nfsDefaultPort is the NFS service port a client uses when the mount options do not name one.
	nfsDefaultPort = 2049

	// ONC RPC (RFC 5531) values used by the NFS NULL call and its reply.
	rpcMsgCall         = 0
	rpcMsgReply        = 1
	rpcVersion         = 2
	rpcProgramNFS      = 100003
	rpcNFSVersion      = 4
	rpcProcNull        = 0
	rpcAuthNone        = 0
	rpcMsgAccepted     = 0
	rpcMsgDenied       = 1
	rpcProgUnavail     = 1
	rpcSystemErr       = 5
	rpcRejectAuthError = 1
	// rpcLastFragment is the last-fragment bit of a record mark (RFC 5531 section 11).
	rpcLastFragment = 1 << 31
	// rpcMaxReplySize bounds the reply read from a server. A reply to the NULL call is a few dozen bytes plus
	// a verifier of at most 400 bytes.
	rpcMaxReplySize = 1024

	// guestNFSProbeTimeout bounds one probe of an NFS service: name lookup, TCP connect, call and reply.
	guestNFSProbeTimeout = 5 * time.Second
	// guestNFSProbeWorkers is the number of NFS services probed at the same time.
	guestNFSProbeWorkers = 8
)

var (
	errNotRPCReply           = errors.New("the reply is not an ONC RPC reply to the NFS NULL call")
	errNFSProgramUnavailable = errors.New("an RPC server answered but does not serve the NFS program")

	// guestNFSProbe probes the NFS service at address. Tests replace it.
	guestNFSProbe = func(ctx context.Context, address string) error {
		return probeNFSService(ctx, address, guestNFSProbeTimeout)
	}
)

// guestNFSServerAddress returns the host:port of the NFS service that pv is mounted from. The host is the
// "server" volume attribute, which is what the node plugin mounts from. The port is the last "port=" mount
// option, or 2049: mount options are read from the PV's mountOptions and then from its "mountOptions" volume
// attribute, the order in which the node plugin passes them to mount. port=0 asks the NFS client to look the
// port up through rpcbind, which is not done here; 2049 is used instead.
func guestNFSServerAddress(pv *v1.PersistentVolume) (string, error) {
	var server, contextMountOptions string
	for k, v := range pv.Spec.CSI.VolumeAttributes {
		switch strings.ToLower(k) {
		case "server":
			server = v
		case "mountoptions":
			contextMountOptions = v
		}
	}
	server = strings.TrimSuffix(strings.TrimPrefix(server, "["), "]")
	if server == "" {
		return "", errors.New("the PV has no server volume attribute")
	}
	port := nfsDefaultPort
	for _, options := range slices.Concat(pv.Spec.MountOptions, []string{contextMountOptions}) {
		for _, option := range strings.Split(options, ",") {
			value, ok := strings.CutPrefix(strings.TrimSpace(option), "port=")
			if !ok {
				continue
			}
			p, err := strconv.Atoi(value)
			if err != nil || p < 0 || p > 65535 {
				return "", fmt.Errorf("invalid mount option %q", option)
			}
			port = p
			if port == 0 {
				port = nfsDefaultPort
			}
		}
	}
	return net.JoinHostPort(server, strconv.Itoa(port)), nil
}

// probeNFSService checks that an NFS service answers at address. It connects over TCP and sends an ONC RPC
// (RFC 5531) call to the NULL procedure of NFS version 4, which takes no arguments, needs no authentication and
// has no effect on the server. It accepts any well-formed reply except one saying that the program is
// unavailable, which comes from an RPC server that does not serve NFS. A reply that the version is not supported
// still comes from an NFS service, so no other version is tried; a denied call, which servers do not normally
// answer NULL with, still comes from an RPC server on the NFS port. A failed name lookup or connect, a reset or
// closed connection, no reply within timeout, or a reply that is not ONC RPC is an error.
//
// This proves only that the NFS service on the server answers from this pod's network. It does not prove that
// worker nodes can reach the server or mount the export (their network path may differ, and an NFSv3 mount
// also needs rpcbind and mountd, which are not probed), that the export or the volume's subdirectory still
// exists, that its permissions allow access, or that existing mounts do not hold stale file handles.
func probeNFSService(ctx context.Context, address string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	// Unblock the call as soon as ctx is cancelled, not only at its deadline.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	xid := rand.Uint32()
	if _, err := conn.Write(nfsNullCall(xid)); err != nil {
		return fmt.Errorf("sending the NFS NULL call to %s: %w", address, err)
	}
	reply, err := readRPCRecord(conn)
	if err != nil {
		return fmt.Errorf("reading the reply to the NFS NULL call from %s: %w", address, err)
	}
	return checkNFSNullReply(reply, xid)
}

// nfsNullCall returns a record-marked RPC call of the NFS version 4 NULL procedure, with AUTH_NONE credentials.
func nfsNullCall(xid uint32) []byte {
	call := make([]byte, 4, 44)
	for _, word := range []uint32{xid, rpcMsgCall, rpcVersion, rpcProgramNFS, rpcNFSVersion, rpcProcNull,
		rpcAuthNone, 0, rpcAuthNone, 0} {
		call = binary.BigEndian.AppendUint32(call, word)
	}
	binary.BigEndian.PutUint32(call, rpcLastFragment|uint32(len(call)-4))
	return call
}

// readRPCRecord reads one record-marked RPC message of at most rpcMaxReplySize bytes.
func readRPCRecord(r io.Reader) ([]byte, error) {
	var record []byte
	for {
		var header [4]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return nil, err
		}
		mark := binary.BigEndian.Uint32(header[:])
		size := mark &^ rpcLastFragment
		if uint64(len(record))+uint64(size) > rpcMaxReplySize {
			return nil, errNotRPCReply
		}
		fragment := make([]byte, size)
		if _, err := io.ReadFull(r, fragment); err != nil {
			return nil, err
		}
		record = append(record, fragment...)
		if mark&rpcLastFragment != 0 {
			return record, nil
		}
	}
}

// checkNFSNullReply checks that reply is the RPC reply to the call with xid, from a server that serves NFS.
func checkNFSNullReply(reply []byte, xid uint32) error {
	r := xdrReader{buf: reply}
	if r.uint32() != xid || r.uint32() != rpcMsgReply {
		return errNotRPCReply
	}
	switch r.uint32() {
	case rpcMsgAccepted:
		r.uint32()         // verifier flavor
		r.skip(r.uint32()) // verifier body
		acceptStat := r.uint32()
		if r.short || acceptStat > rpcSystemErr {
			return errNotRPCReply
		}
		if acceptStat == rpcProgUnavail {
			return errNFSProgramUnavailable
		}
		return nil
	case rpcMsgDenied:
		if rejectStat := r.uint32(); r.short || rejectStat > rpcRejectAuthError {
			return errNotRPCReply
		}
		return nil
	default:
		return errNotRPCReply
	}
}

// xdrReader decodes XDR (RFC 4506) items from buf. Reading past the end yields zeroes and sets short.
type xdrReader struct {
	buf   []byte
	short bool
}

func (r *xdrReader) uint32() uint32 {
	if len(r.buf) < 4 {
		r.short = true
		return 0
	}
	v := binary.BigEndian.Uint32(r.buf)
	r.buf = r.buf[4:]
	return v
}

// skip skips n bytes of opaque data and their padding to a multiple of four bytes.
func (r *xdrReader) skip(n uint32) {
	padded := (uint64(n) + 3) &^ 3
	if uint64(len(r.buf)) < padded {
		r.short = true
		r.buf = nil
		return
	}
	r.buf = r.buf[padded:]
}

// probeGuestNFSServers probes each address once with guestNFSProbe, at most guestNFSProbeWorkers at a time, until
// ctx is done. It returns the result of every probe that completed: nil when the NFS service answered. An address
// whose probe did not start, or failed because ctx ended, has no result: its health is unknown.
func probeGuestNFSServers(ctx context.Context, addresses []string) map[string]error {
	results := make(map[string]error, len(addresses))
	var mu sync.Mutex
	var wg sync.WaitGroup
	queue := make(chan string)
	for range min(guestNFSProbeWorkers, len(addresses)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for address := range queue {
				if ctx.Err() != nil {
					continue
				}
				err := guestNFSProbe(ctx, address)
				if err != nil && ctx.Err() != nil {
					continue
				}
				mu.Lock()
				results[address] = err
				mu.Unlock()
			}
		}()
	}
enqueue:
	for _, address := range addresses {
		select {
		case queue <- address:
		case <-ctx.Done():
			break enqueue
		}
	}
	close(queue)
	wg.Wait()
	return results
}
