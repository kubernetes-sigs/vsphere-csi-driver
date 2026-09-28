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

// Package nfsmount implements just enough of ONC RPC (RFC 5531) and the NFSv3 MOUNT
// protocol (RFC 1813) to answer one question - "would mounting server:/share succeed
// right now?" - without an actual mount(2) syscall (which needs CAP_SYS_ADMIN/root,
// unavailable to the unprivileged syncer container this runs in) and without a full
// third-party NFS client dependency (only one narrow RPC procedure is needed here, not
// file I/O).
package nfsmount

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	rpcVersion    = 2
	msgTypeCall   = 0
	msgTypeReply  = 1
	replyAccepted = 0
	acceptSuccess = 0

	authFlavorNone = 0

	// maxReplySize bounds how much a single RPC reply is allowed to be, so a
	// misbehaving or malicious server can't make readReply allocate unbounded memory.
	maxReplySize = 64 * 1024
)

// xdrWriter accumulates XDR-encoded bytes for one RPC call body.
type xdrWriter struct {
	buf []byte
}

func (w *xdrWriter) uint32(v uint32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, v)
}

// opaqueAuthNone XDR-encodes an empty AUTH_NONE opaque_auth value (the credential or
// verifier field of an RPC call) - flavor 0, zero-length body.
func (w *xdrWriter) opaqueAuthNone() {
	w.uint32(authFlavorNone)
	w.uint32(0)
}

// xdrString XDR-encodes a variable-length opaque/string: a uint32 length prefix
// followed by the bytes, zero-padded to a 4-byte boundary.
func (w *xdrWriter) xdrString(s string) {
	w.uint32(uint32(len(s)))
	w.buf = append(w.buf, s...)
	if pad := (4 - len(s)%4) % 4; pad > 0 {
		w.buf = append(w.buf, make([]byte, pad)...)
	}
}

// xdrReader reads XDR-encoded fields out of an RPC reply body.
type xdrReader struct {
	buf []byte
	off int
}

func (r *xdrReader) uint32() (uint32, error) {
	if r.off+4 > len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(r.buf[r.off:])
	r.off += 4
	return v, nil
}

// skipOpaque skips a variable-length opaque field (uint32 length + padded bytes) whose
// contents this package doesn't need to inspect - e.g. a MOUNT MNT reply's file handle.
func (r *xdrReader) skipOpaque() error {
	n, err := r.uint32()
	if err != nil {
		return err
	}
	padded := int(n) + (4-int(n)%4)%4
	if r.off+padded > len(r.buf) {
		return io.ErrUnexpectedEOF
	}
	r.off += padded
	return nil
}

// skipOpaqueAuth skips an opaque_auth field (uint32 flavor + opaque body, i.e. flavor
// followed by the same length-prefixed-and-padded shape as skipOpaque) - the RPC
// reply's verifier is this type, distinct from a bare opaque field like a file handle.
func (r *xdrReader) skipOpaqueAuth() error {
	if _, err := r.uint32(); err != nil { // flavor
		return err
	}
	return r.skipOpaque()
}

// rpcCall sends one ONC RPC call over conn (record-marked per RFC 5531 section 11,
// with AUTH_NONE credentials) and returns the raw bytes of the reply's
// procedure-specific result - i.e. with the RPC call/reply envelope already parsed
// and stripped, and accept_stat already checked.
func rpcCall(conn net.Conn, xid, prog, vers, proc uint32, args []byte) ([]byte, error) {
	w := &xdrWriter{}
	w.uint32(xid)
	w.uint32(msgTypeCall)
	w.uint32(rpcVersion)
	w.uint32(prog)
	w.uint32(vers)
	w.uint32(proc)
	w.opaqueAuthNone() // cred
	w.opaqueAuthNone() // verf
	w.buf = append(w.buf, args...)

	// Record marking: a single fragment, high bit set to mark it as the last one.
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 0x80000000|uint32(len(w.buf)))
	if _, err := conn.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write RPC record header: %w", err)
	}
	if _, err := conn.Write(w.buf); err != nil {
		return nil, fmt.Errorf("failed to write RPC call body: %w", err)
	}

	return readReply(conn, xid)
}

// readReply reads one record-marked RPC reply from conn, validates it against xid and
// the RPC accept/reject status, and returns the procedure-specific result bytes that
// follow a successful accept_stat.
func readReply(conn net.Conn, xid uint32) ([]byte, error) {
	var fragment []byte
	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return nil, fmt.Errorf("failed to read RPC record header: %w", err)
		}
		h := binary.BigEndian.Uint32(header)
		last := h&0x80000000 != 0
		length := h &^ 0x80000000
		if len(fragment)+int(length) > maxReplySize {
			return nil, fmt.Errorf("RPC reply exceeds %d byte limit", maxReplySize)
		}

		buf := make([]byte, length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return nil, fmt.Errorf("failed to read RPC record body: %w", err)
		}
		fragment = append(fragment, buf...)
		if last {
			break
		}
	}

	r := &xdrReader{buf: fragment}
	replyXid, err := r.uint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read reply xid: %w", err)
	}
	if replyXid != xid {
		return nil, fmt.Errorf("RPC reply xid %d does not match call xid %d", replyXid, xid)
	}
	msgType, err := r.uint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read reply msg_type: %w", err)
	}
	if msgType != msgTypeReply {
		return nil, fmt.Errorf("unexpected RPC msg_type %d in reply", msgType)
	}
	replyStat, err := r.uint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read reply_stat: %w", err)
	}
	if replyStat != replyAccepted {
		return nil, fmt.Errorf("RPC call was denied (reply_stat=%d)", replyStat)
	}
	if err := r.skipOpaqueAuth(); err != nil { // verifier
		return nil, fmt.Errorf("failed to read reply verifier: %w", err)
	}
	acceptStat, err := r.uint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read accept_stat: %w", err)
	}
	if acceptStat != acceptSuccess {
		return nil, fmt.Errorf("RPC call rejected with accept_stat=%d", acceptStat)
	}
	return r.buf[r.off:], nil
}
