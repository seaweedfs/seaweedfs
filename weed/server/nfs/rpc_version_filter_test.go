package nfs

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// buildRPCCallFrame constructs a single TCP-framed RPC CALL header without
// procedure arguments — enough for the version filter to decide whether to
// reject the connection. The frame layout matches RFC 5531 (Open Network
// Computing RPC v2): a 4-byte fragment marker (last-fragment bit set on a
// 40-byte body) followed by xid + msg_type=CALL + rpcvers=2 + prog + vers +
// proc + two empty AUTH_NONE opaque_auth structs.
func buildRPCCallFrame(xid, prog, vers, proc uint32) []byte {
	const bodyLen = 40
	frame := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(frame[0:4], uint32(bodyLen)|(1<<31))
	binary.BigEndian.PutUint32(frame[4:8], xid)
	binary.BigEndian.PutUint32(frame[8:12], 0) // msg_type CALL
	binary.BigEndian.PutUint32(frame[12:16], 2)
	binary.BigEndian.PutUint32(frame[16:20], prog)
	binary.BigEndian.PutUint32(frame[20:24], vers)
	binary.BigEndian.PutUint32(frame[24:28], proc)
	// cred + verf both AUTH_NONE / length 0
	return frame
}

// readPROGMismatchReply parses a TCP-framed PROG_MISMATCH reply produced by
// writeProgMismatchTCP and returns the xid plus the supported (low, high)
// version range advertised by the server.
func readPROGMismatchReply(t *testing.T, conn net.Conn) (xid, low, high uint32) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4+progMismatchBodyLen)
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		t.Fatalf("read reply: %v (got %d bytes)", err, n)
	}
	frag := binary.BigEndian.Uint32(buf[0:4])
	if frag&(1<<31) == 0 {
		t.Fatalf("reply frame missing last-fragment bit: %x", frag)
	}
	if got := frag &^ (1 << 31); got != progMismatchBodyLen {
		t.Fatalf("reply body length=%d want %d", got, progMismatchBodyLen)
	}
	xid = binary.BigEndian.Uint32(buf[4:8])
	if mt := binary.BigEndian.Uint32(buf[8:12]); mt != 1 {
		t.Fatalf("reply msg_type=%d want REPLY(1)", mt)
	}
	if rs := binary.BigEndian.Uint32(buf[12:16]); rs != 0 {
		t.Fatalf("reply reply_stat=%d want MSG_ACCEPTED(0)", rs)
	}
	if as := binary.BigEndian.Uint32(buf[24:28]); as != 2 {
		t.Fatalf("reply accept_stat=%d want PROG_MISMATCH(2)", as)
	}
	low = binary.BigEndian.Uint32(buf[28:32])
	high = binary.BigEndian.Uint32(buf[32:36])
	return
}

func TestVersionFilterRejectsNFSv4WithProgMismatch(t *testing.T) {
	innerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer innerListener.Close()

	listener := newVersionFilterListener(innerListener)

	// In a real server, accepted conns are passed to go-nfs. We just need
	// to drive Accept() so the filter runs; the test never sees a wrapped
	// conn because the v4 frame is rejected.
	accepted := make(chan net.Conn, 1)
	go func() {
		for {
			c, aerr := listener.Accept()
			if aerr != nil {
				return
			}
			accepted <- c
		}
	}()

	conn, err := net.Dial("tcp", innerListener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// NFSv4 NULL: the first probe Linux mount.nfs sends when trying v4.
	if _, err := conn.Write(buildRPCCallFrame(0xdeadbeef, nfsProgram, 4, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}

	xid, low, high := readPROGMismatchReply(t, conn)
	if xid != 0xdeadbeef {
		t.Errorf("xid=%x want %x", xid, 0xdeadbeef)
	}
	if low != supportedNFSVer || high != supportedNFSVer {
		t.Errorf("supported range=(%d,%d) want (%d,%d)", low, high, supportedNFSVer, supportedNFSVer)
	}

	// Filter must close the connection after replying so the client knows
	// not to send another RPC on this socket. Insist on io.EOF specifically:
	// "any error" would let a stuck (but still-open) connection pass this
	// check via a deadline timeout, which is exactly the regression we want
	// to catch.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	one := make([]byte, 1)
	n, err := conn.Read(one)
	switch {
	case err == nil:
		t.Errorf("expected EOF after PROG_MISMATCH but read returned %d bytes", n)
	case !errors.Is(err, io.EOF):
		t.Errorf("expected io.EOF after PROG_MISMATCH, got %v (likely a regression where the filter replies but does not close)", err)
	}

	select {
	case c := <-accepted:
		c.Close()
		t.Error("rejected connection should not be returned to caller")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVersionFilterRejectsMOUNTv4WithProgMismatch(t *testing.T) {
	innerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer innerListener.Close()

	listener := newVersionFilterListener(innerListener)
	go func() {
		for {
			c, aerr := listener.Accept()
			if aerr != nil {
				return
			}
			c.Close()
		}
	}()

	conn, err := net.Dial("tcp", innerListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write(buildRPCCallFrame(42, mountProgram, 4, 0)); err != nil {
		t.Fatal(err)
	}

	xid, low, high := readPROGMismatchReply(t, conn)
	if xid != 42 {
		t.Errorf("xid=%d want 42", xid)
	}
	if low != supportedNFSVer || high != supportedNFSVer {
		t.Errorf("supported range=(%d,%d) want (3,3)", low, high)
	}
}

func TestVersionFilterPassesThroughNFSv3(t *testing.T) {
	innerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer innerListener.Close()

	listener := newVersionFilterListener(innerListener)
	got := make(chan []byte, 1)
	go func() {
		c, aerr := listener.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 44)
		_, rerr := io.ReadFull(c, buf)
		if rerr != nil {
			return
		}
		got <- buf
	}()

	conn, err := net.Dial("tcp", innerListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	frame := buildRPCCallFrame(7, nfsProgram, 3, 0)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-got:
		if string(received) != string(frame) {
			t.Error("v3 frame was modified or partially consumed by filter")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("v3 frame not delivered to inner accept handler")
	}
}

func TestVersionFilterPassesThroughUnknownProgram(t *testing.T) {
	// The filter should only police NFS / MOUNT versions; other programs
	// reach go-nfs which already responds PROG_UNAVAIL itself. Otherwise
	// adding a new program (e.g. NLM) here would require updating the
	// filter, which would defeat the point of using it as a thin shim.
	innerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer innerListener.Close()

	listener := newVersionFilterListener(innerListener)
	delivered := make(chan struct{}, 1)
	go func() {
		c, aerr := listener.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 44)
		if _, rerr := io.ReadFull(c, buf); rerr == nil {
			delivered <- struct{}{}
		}
	}()

	conn, err := net.Dial("tcp", innerListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Program 100021 is NLM, which weed nfs doesn't run; let go-nfs handle
	// the unsupported-program reply.
	if _, err := conn.Write(buildRPCCallFrame(99, 100021, 4, 0)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("unknown-program frame should pass through filter")
	}
}

func TestVersionFilterIgnoresShortFirstFragment(t *testing.T) {
	// Peek(28) can read past the first fragment's body when the body is
	// shorter than the 24-byte fixed RPC CALL header. Without a length
	// check, the prog/vers fields would be sourced from bytes belonging to
	// the *next* RPC (or a syntactic accident), and the filter could
	// spuriously reject the connection. Send a 12-byte first fragment whose
	// trailing peek-region bytes look like an NFSv4 CALL header, and assert
	// the filter does NOT emit a PROG_MISMATCH reply.
	innerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer innerListener.Close()

	listener := newVersionFilterListener(innerListener)
	go func() {
		for {
			c, aerr := listener.Accept()
			if aerr != nil {
				return
			}
			c.Close()
		}
	}()

	conn, err := net.Dial("tcp", innerListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const shortBody = 12
	payload := make([]byte, 4+24)
	binary.BigEndian.PutUint32(payload[0:4], shortBody|(1<<31)) // last-fragment, body=12
	// Bytes 4..16 are the actual fragment body (12 bytes — too short for a
	// CALL header; the filter must not look at them as one).
	// Bytes 16..28 sit past the fragment in the peek window. If we were to
	// (incorrectly) read prog/vers from hdr[16:24], we'd see NFS+v4 here.
	binary.BigEndian.PutUint32(payload[16:20], nfsProgram)
	binary.BigEndian.PutUint32(payload[20:24], 4)

	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}

	// If the filter erroneously rejected, it would send a 36-byte TCP RPC
	// reply (4-byte frag marker + 32-byte PROG_MISMATCH body) within ms.
	// Wait briefly and assert nothing PROG_MISMATCH-shaped came back.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	hdr := make([]byte, 4)
	n, err := io.ReadFull(conn, hdr)
	if err == nil && n == 4 {
		if got := binary.BigEndian.Uint32(hdr); got == uint32(progMismatchBodyLen)|(1<<31) {
			t.Fatal("filter sent PROG_MISMATCH on a short fragment whose trailing peek bytes only superficially resembled a v4 call")
		}
	}
	// Anything else (timeout, EOF, or unrelated bytes) is fine — we only
	// care that the filter did NOT misclassify the short fragment.
}

func TestVersionFilterDoesNotHeadOfLineBlockOnSlowConn(t *testing.T) {
	// Regression test: the previous implementation peeked the first RPC
	// frame inline in Accept(), so an idle TCP-only connect would block
	// every later Accept() call for up to rpcVersionFilterPeekTimeout.
	// The peek now runs in a per-conn goroutine; a fast follow-up connect
	// must reach the inner accept handler well before the slow conn's
	// peek deadline.
	innerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer innerListener.Close()

	listener := newVersionFilterListener(innerListener)

	delivered := make(chan struct{}, 1)
	go func() {
		c, aerr := listener.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 44)
		if _, rerr := io.ReadFull(c, buf); rerr == nil {
			delivered <- struct{}{}
		}
	}()

	// Slow client: connect, never write. Holds a goroutine inside the
	// filter peeking until the deadline, but must not block the next conn.
	slowConn, err := net.Dial("tcp", innerListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer slowConn.Close()

	// Fast client: send a valid v3 frame straight away; this conn must be
	// delivered to the inner accept handler without waiting for slowConn.
	fastConn, err := net.Dial("tcp", innerListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer fastConn.Close()

	if _, err := fastConn.Write(buildRPCCallFrame(11, nfsProgram, 3, 0)); err != nil {
		t.Fatal(err)
	}

	// Bound the wait well below rpcVersionFilterPeekTimeout (10s) so a
	// regression to inline peeking would clearly time out here.
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("fast conn should not be head-of-line blocked by slow conn's peek")
	}
}
