package nfs

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func buildRPCCall(t *testing.T, xid, prog, vers, proc uint32, credBody, verfBody, args []byte) []byte {
	t.Helper()
	pad := func(b []byte) []byte {
		r := len(b) % 4
		if r == 0 {
			return b
		}
		out := make([]byte, len(b)+4-r)
		copy(out, b)
		return out
	}
	buf := new(bytes.Buffer)
	write := func(v uint32) {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	write(xid)
	write(rpcMsgCall)
	write(2) // rpcvers
	write(prog)
	write(vers)
	write(proc)
	// cred
	write(rpcAuthNone)
	write(uint32(len(credBody)))
	buf.Write(pad(credBody))
	// verf
	write(rpcAuthNone)
	write(uint32(len(verfBody)))
	buf.Write(pad(verfBody))
	buf.Write(args)
	return buf.Bytes()
}

func parseAcceptedReply(t *testing.T, reply []byte) (xid, acceptStat uint32, body []byte) {
	t.Helper()
	if len(reply) < 24 {
		t.Fatalf("reply too short: %d bytes", len(reply))
	}
	xid = binary.BigEndian.Uint32(reply[0:4])
	if mt := binary.BigEndian.Uint32(reply[4:8]); mt != rpcMsgReply {
		t.Fatalf("msg_type=%d, want REPLY", mt)
	}
	if rs := binary.BigEndian.Uint32(reply[8:12]); rs != rpcMsgAccepted {
		t.Fatalf("reply_stat=%d, want ACCEPTED", rs)
	}
	// verf
	verfLen := binary.BigEndian.Uint32(reply[16:20])
	if verfLen != 0 {
		t.Fatalf("unexpected verf length %d", verfLen)
	}
	acceptStat = binary.BigEndian.Uint32(reply[20:24])
	body = reply[24:]
	return
}

func newTestPortmap() *portmapServer {
	return newPortmapServer("127.0.0.1", portmapPort, 2049)
}

func TestParseRPCCall_SkipsAuth(t *testing.T) {
	cred := []byte("hello") // 5 bytes -> padded to 8
	verf := []byte{}
	args := []byte{0x01, 0x02, 0x03, 0x04}
	msg := buildRPCCall(t, 42, portmapProgram, portmapVersion, pmapProcNull, cred, verf, args)

	xid, prog, vers, proc, gotArgs, err := parseRPCCall(msg)
	if err != nil {
		t.Fatalf("parseRPCCall: %v", err)
	}
	if xid != 42 || prog != portmapProgram || vers != portmapVersion || proc != pmapProcNull {
		t.Fatalf("header mismatch: xid=%d prog=%d vers=%d proc=%d", xid, prog, vers, proc)
	}
	if !bytes.Equal(gotArgs, args) {
		t.Fatalf("args mismatch: got %x want %x", gotArgs, args)
	}
}

func TestParseRPCCall_RejectsReply(t *testing.T) {
	buf := make([]byte, 40)
	binary.BigEndian.PutUint32(buf[4:8], rpcMsgReply)
	if _, _, _, _, _, err := parseRPCCall(buf); err == nil {
		t.Fatal("expected error on reply-typed message")
	}
}

func TestParseRPCCall_TruncatedAuth(t *testing.T) {
	// Claim huge cred length but provide no body.
	buf := make([]byte, 40)
	binary.BigEndian.PutUint32(buf[4:8], rpcMsgCall)
	binary.BigEndian.PutUint32(buf[8:12], 2)
	binary.BigEndian.PutUint32(buf[28:32], 1000) // cred len
	if _, _, _, _, _, err := parseRPCCall(buf); err == nil {
		t.Fatal("expected error on truncated auth")
	}
}

func TestHandleCall_Null(t *testing.T) {
	ps := newTestPortmap()
	msg := buildRPCCall(t, 7, portmapProgram, portmapVersion, pmapProcNull, nil, nil, nil)
	reply := ps.handleCall(msg)
	xid, acc, body := parseAcceptedReply(t, reply)
	if xid != 7 || acc != rpcAcceptSuccess || len(body) != 0 {
		t.Fatalf("null reply xid=%d acc=%d body=%x", xid, acc, body)
	}
}

func TestHandleCall_GetPort_HitAndMiss(t *testing.T) {
	ps := newTestPortmap()

	buildQuery := func(prog, vers, prot uint32) []byte {
		args := make([]byte, 16)
		binary.BigEndian.PutUint32(args[0:4], prog)
		binary.BigEndian.PutUint32(args[4:8], vers)
		binary.BigEndian.PutUint32(args[8:12], prot)
		// port field is ignored by the server; leave zero
		return args
	}

	cases := []struct {
		name             string
		prog, vers, prot uint32
		wantPort         uint32
	}{
		{"nfs-v3-tcp-hit", nfsProgram, 3, ipProtoTCP, 2049},
		{"mount-v3-tcp-hit", mountProgram, 3, ipProtoTCP, 2049},
		{"mount-v3-udp-hit", mountProgram, 3, ipProtoUDP, 2049},
		{"mount-v1-tcp-miss", mountProgram, 1, ipProtoTCP, 0},
		{"nfs-v3-udp-miss", nfsProgram, 3, ipProtoUDP, 0},
		{"nlm-miss", 100021, 4, ipProtoTCP, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := buildRPCCall(t, 11, portmapProgram, portmapVersion, pmapProcGetPort, nil, nil, buildQuery(tc.prog, tc.vers, tc.prot))
			reply := ps.handleCall(msg)
			xid, acc, body := parseAcceptedReply(t, reply)
			if xid != 11 {
				t.Fatalf("xid=%d want 11", xid)
			}
			if acc != rpcAcceptSuccess {
				t.Fatalf("acc=%d want SUCCESS", acc)
			}
			if len(body) != 4 {
				t.Fatalf("getport body len=%d want 4", len(body))
			}
			got := binary.BigEndian.Uint32(body)
			if got != tc.wantPort {
				t.Fatalf("port=%d want %d", got, tc.wantPort)
			}
		})
	}
}

func TestHandleCall_Dump(t *testing.T) {
	ps := newTestPortmap()
	msg := buildRPCCall(t, 13, portmapProgram, portmapVersion, pmapProcDump, nil, nil, nil)
	reply := ps.handleCall(msg)
	_, acc, body := parseAcceptedReply(t, reply)
	if acc != rpcAcceptSuccess {
		t.Fatalf("acc=%d", acc)
	}
	var entries []portmapEntry
	p := 0
	for p+4 <= len(body) {
		vf := binary.BigEndian.Uint32(body[p : p+4])
		p += 4
		if vf == 0 {
			break
		}
		if p+16 > len(body) {
			t.Fatalf("truncated entry at %d", p)
		}
		entries = append(entries, portmapEntry{
			Program:  binary.BigEndian.Uint32(body[p : p+4]),
			Version:  binary.BigEndian.Uint32(body[p+4 : p+8]),
			Protocol: binary.BigEndian.Uint32(body[p+8 : p+12]),
			Port:     binary.BigEndian.Uint32(body[p+12 : p+16]),
		})
		p += 16
	}
	if len(entries) != 3 {
		t.Fatalf("got %d dump entries, want 3: %+v", len(entries), entries)
	}
	wantSet := map[portmapEntry]bool{
		{Program: nfsProgram, Version: 3, Protocol: ipProtoTCP, Port: 2049}:   false,
		{Program: mountProgram, Version: 3, Protocol: ipProtoTCP, Port: 2049}: false,
		{Program: mountProgram, Version: 3, Protocol: ipProtoUDP, Port: 2049}: false,
	}
	for _, e := range entries {
		if _, ok := wantSet[e]; !ok {
			t.Fatalf("unexpected dump entry %+v", e)
		}
		wantSet[e] = true
	}
	for e, seen := range wantSet {
		if !seen {
			t.Fatalf("missing dump entry %+v", e)
		}
	}
}

func TestHandleCall_UnknownProg(t *testing.T) {
	ps := newTestPortmap()
	msg := buildRPCCall(t, 1, 999999, 1, 0, nil, nil, nil)
	reply := ps.handleCall(msg)
	_, acc, _ := parseAcceptedReply(t, reply)
	if acc != rpcAcceptProgUnavail {
		t.Fatalf("acc=%d want PROG_UNAVAIL", acc)
	}
}

func TestHandleCall_VersionMismatch(t *testing.T) {
	ps := newTestPortmap()
	msg := buildRPCCall(t, 1, portmapProgram, 42, pmapProcNull, nil, nil, nil)
	reply := ps.handleCall(msg)
	_, acc, body := parseAcceptedReply(t, reply)
	if acc != rpcAcceptProgMismatch {
		t.Fatalf("acc=%d want PROG_MISMATCH", acc)
	}
	if len(body) != 8 {
		t.Fatalf("mismatch body len=%d want 8", len(body))
	}
	lo := binary.BigEndian.Uint32(body[0:4])
	hi := binary.BigEndian.Uint32(body[4:8])
	if lo != portmapVersion || hi != portmapVersion {
		t.Fatalf("mismatch range lo=%d hi=%d", lo, hi)
	}
}

func TestHandleCall_UnknownProc(t *testing.T) {
	ps := newTestPortmap()
	msg := buildRPCCall(t, 1, portmapProgram, portmapVersion, 42, nil, nil, nil)
	reply := ps.handleCall(msg)
	_, acc, _ := parseAcceptedReply(t, reply)
	if acc != rpcAcceptProcUnavail {
		t.Fatalf("acc=%d want PROC_UNAVAIL", acc)
	}
}

func TestHandleCall_SetRefused(t *testing.T) {
	ps := newTestPortmap()
	args := make([]byte, 16) // mapping struct
	msg := buildRPCCall(t, 1, portmapProgram, portmapVersion, pmapProcSet, nil, nil, args)
	reply := ps.handleCall(msg)
	_, acc, body := parseAcceptedReply(t, reply)
	if acc != rpcAcceptSuccess {
		t.Fatalf("acc=%d", acc)
	}
	if len(body) != 4 || binary.BigEndian.Uint32(body) != 0 {
		t.Fatalf("PMAP_SET must return FALSE, got %x", body)
	}
}

// pickFreePort asks the OS for an unused high port by opening and closing a
// listener on it. Used so the end-to-end tests can run in parallel without
// stepping on the privileged default port 111.
func pickFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestPortmapServer_UDPGetPort(t *testing.T) {
	port := pickFreePort(t)
	ps := newPortmapServer("127.0.0.1", port, 2049)
	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })

	args := make([]byte, 16)
	binary.BigEndian.PutUint32(args[0:4], nfsProgram)
	binary.BigEndian.PutUint32(args[4:8], 3)
	binary.BigEndian.PutUint32(args[8:12], ipProtoTCP)
	msg := buildRPCCall(t, 99, portmapProgram, portmapVersion, pmapProcGetPort, nil, nil, args)

	conn, err := net.Dial("udp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial udp: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	xid, acc, body := parseAcceptedReply(t, buf[:n])
	if xid != 99 || acc != rpcAcceptSuccess || len(body) != 4 {
		t.Fatalf("bad reply xid=%d acc=%d body=%x", xid, acc, body)
	}
	if got := binary.BigEndian.Uint32(body); got != 2049 {
		t.Fatalf("udp getport port=%d want 2049", got)
	}
}

func TestPortmapServer_CloseEvictsIdleTCPConn(t *testing.T) {
	port := pickFreePort(t)
	ps := newPortmapServer("127.0.0.1", port, 2049)
	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		_ = ps.Close()
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Issue one call and read its reply so the server-side connection is
	// definitely registered before we trigger shutdown.
	msg := buildRPCCall(t, 1, portmapProgram, portmapVersion, pmapProcNull, nil, nil, nil)
	var mark [4]byte
	binary.BigEndian.PutUint32(mark[:], uint32(len(msg))|(1<<31))
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(mark[:]); err != nil {
		t.Fatalf("write mark: %v", err)
	}
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write msg: %v", err)
	}
	if _, err := io.ReadFull(conn, mark[:]); err != nil {
		t.Fatalf("read mark: %v", err)
	}
	rlen := binary.BigEndian.Uint32(mark[:]) &^ (1 << 31)
	if _, err := io.ReadFull(conn, make([]byte, rlen)); err != nil {
		t.Fatalf("read body: %v", err)
	}

	// Close must return long before the TCP idle deadline (30s) — in
	// other words, the server must actively close the idle conn rather
	// than wait for the deadline or for the client to disconnect.
	done := make(chan error, 1)
	go func() { done <- ps.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s; in-flight conn not evicted")
	}

	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	if _, err := conn.Read(make([]byte, 4)); err == nil {
		t.Fatal("expected read error on client conn after server Close")
	}
}

func TestPortmapServer_TCPGetPort(t *testing.T) {
	port := pickFreePort(t)
	ps := newPortmapServer("127.0.0.1", port, 2049)
	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close() })

	args := make([]byte, 16)
	binary.BigEndian.PutUint32(args[0:4], mountProgram)
	binary.BigEndian.PutUint32(args[4:8], 3)
	binary.BigEndian.PutUint32(args[8:12], ipProtoTCP)
	msg := buildRPCCall(t, 123, portmapProgram, portmapVersion, pmapProcGetPort, nil, nil, args)

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial tcp: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	// record mark: last-fragment bit + length
	var mark [4]byte
	binary.BigEndian.PutUint32(mark[:], uint32(len(msg))|(1<<31))
	if _, err := conn.Write(mark[:]); err != nil {
		t.Fatalf("write mark: %v", err)
	}
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write msg: %v", err)
	}

	var rmark [4]byte
	if _, err := io.ReadFull(conn, rmark[:]); err != nil {
		t.Fatalf("read mark: %v", err)
	}
	rlen := binary.BigEndian.Uint32(rmark[:]) &^ (1 << 31)
	buf := make([]byte, rlen)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read body: %v", err)
	}
	xid, acc, body := parseAcceptedReply(t, buf)
	if xid != 123 || acc != rpcAcceptSuccess || len(body) != 4 {
		t.Fatalf("bad reply xid=%d acc=%d body=%x", xid, acc, body)
	}
	if got := binary.BigEndian.Uint32(body); got != 2049 {
		t.Fatalf("tcp getport port=%d want 2049", got)
	}
}
