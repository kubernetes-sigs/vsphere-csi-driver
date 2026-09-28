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

package nfsmount

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestXDRWriterUint32(t *testing.T) {
	w := &xdrWriter{}
	w.uint32(1)
	w.uint32(0xdeadbeef)
	if len(w.buf) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(w.buf))
	}
	if binary.BigEndian.Uint32(w.buf[0:4]) != 1 {
		t.Errorf("first uint32 encoded incorrectly")
	}
	if binary.BigEndian.Uint32(w.buf[4:8]) != 0xdeadbeef {
		t.Errorf("second uint32 encoded incorrectly")
	}
}

func TestXDRStringPadding(t *testing.T) {
	tests := []struct {
		s           string
		wantPadding int
	}{
		{"", 0},
		{"a", 3},
		{"ab", 2},
		{"abc", 1},
		{"abcd", 0},
		{"/store", 2}, // 6 bytes -> pad to 8
	}
	for _, tc := range tests {
		w := &xdrWriter{}
		w.xdrString(tc.s)
		wantLen := 4 + len(tc.s) + tc.wantPadding
		if len(w.buf) != wantLen {
			t.Errorf("xdrString(%q): got len %d, want %d", tc.s, len(w.buf), wantLen)
		}
		r := &xdrReader{buf: w.buf}
		n, err := r.uint32()
		if err != nil {
			t.Fatalf("xdrString(%q): failed to read length prefix: %v", tc.s, err)
		}
		if int(n) != len(tc.s) {
			t.Errorf("xdrString(%q): length prefix = %d, want %d", tc.s, n, len(tc.s))
		}
	}
}

func TestSkipOpaque(t *testing.T) {
	w := &xdrWriter{}
	w.xdrString("filehandle-bytes")
	w.uint32(0x12345678) // sentinel value after the opaque field

	r := &xdrReader{buf: w.buf}
	if err := r.skipOpaque(); err != nil {
		t.Fatalf("skipOpaque failed: %v", err)
	}
	sentinel, err := r.uint32()
	if err != nil {
		t.Fatalf("failed to read sentinel after skipOpaque: %v", err)
	}
	if sentinel != 0x12345678 {
		t.Errorf("skipOpaque left reader at wrong offset: got sentinel %x, want %x", sentinel, 0x12345678)
	}
}

func TestUint32TruncatedBuffer(t *testing.T) {
	r := &xdrReader{buf: []byte{0x00, 0x01}}
	if _, err := r.uint32(); err == nil {
		t.Error("expected error reading uint32 from truncated buffer, got nil")
	}
}

// fakeRPCServer accepts exactly one TCP connection, reads one record-marked RPC call
// (ignoring its contents beyond the xid, which the reply must echo back per RFC 5531),
// and replies with the given resultBytes wrapped in a successful RPC reply envelope
// (or, if acceptStat is non-zero, an envelope reporting that accept_stat instead).
func fakeRPCServer(t *testing.T, acceptStat uint32, resultBytes []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start fake RPC server: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read the single record-marked request just to extract the xid; the fake
		// server doesn't need to validate the rest of the call.
		header := make([]byte, 4)
		if _, err := readFull(conn, header); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(header) &^ 0x80000000
		body := make([]byte, length)
		if _, err := readFull(conn, body); err != nil {
			return
		}
		xid := binary.BigEndian.Uint32(body[0:4])

		w := &xdrWriter{}
		w.uint32(xid)
		w.uint32(msgTypeReply)
		w.uint32(replyAccepted)
		w.opaqueAuthNone() // verifier
		w.uint32(acceptStat)
		if acceptStat == acceptSuccess {
			w.buf = append(w.buf, resultBytes...)
		}

		replyHeader := make([]byte, 4)
		binary.BigEndian.PutUint32(replyHeader, 0x80000000|uint32(len(w.buf)))
		_, _ = conn.Write(replyHeader)
		_, _ = conn.Write(w.buf)
	}()

	return ln.Addr().String()
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func TestRpcCallSuccess(t *testing.T) {
	want := []byte{0x00, 0x00, 0x00, 0x2a} // an arbitrary uint32 result: 42
	addr := fakeRPCServer(t, acceptSuccess, want)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to dial fake server: %v", err)
	}
	defer conn.Close()

	result, err := rpcCall(conn, 7, 100005, 3, 1, nil)
	if err != nil {
		t.Fatalf("rpcCall failed: %v", err)
	}
	if len(result) != len(want) {
		t.Fatalf("result length = %d, want %d", len(result), len(want))
	}
	for i := range want {
		if result[i] != want[i] {
			t.Errorf("result[%d] = %x, want %x", i, result[i], want[i])
		}
	}
}

func TestRpcCallAcceptStatError(t *testing.T) {
	addr := fakeRPCServer(t, 2 /* PROG_MISMATCH */, nil)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to dial fake server: %v", err)
	}
	defer conn.Close()

	if _, err := rpcCall(conn, 7, 100005, 3, 1, nil); err == nil {
		t.Error("expected error for non-success accept_stat, got nil")
	}
}

func TestRpcCallConnectionRefused(t *testing.T) {
	// Nothing listens here - port 0 dialed directly is invalid/refused immediately.
	conn, err := net.DialTimeout("tcp", "127.0.0.1:1", 200*time.Millisecond)
	if err == nil {
		defer conn.Close()
		t.Skip("port 1 unexpectedly accepted a connection in this environment")
	}
}
