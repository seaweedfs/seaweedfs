package nfs

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gonfs "github.com/willscott/go-nfs"
)

// buildMountCallFrame constructs a MOUNT v3 RPC CALL with an opaque dirpath
// argument. The shape matches RFC 5531 §9: xid + msg_type=CALL + rpcvers=2 +
// prog + vers + proc + cred(AUTH_NONE) + verf(AUTH_NONE) + arg.
func buildMountCallFrame(xid, prog, vers, proc uint32, dirpath string) []byte {
	// RPC CALL header (24 bytes) + 2 × AUTH_NONE opaque_auth (16 bytes) +
	// dirpath as XDR opaque (4-byte length + padded body).
	dpLen := uint32(len(dirpath))
	dpPadded := (dpLen + 3) &^ 3
	out := make([]byte, 24+16+4+dpPadded)
	binary.BigEndian.PutUint32(out[0:4], xid)
	binary.BigEndian.PutUint32(out[4:8], rpcMsgCall)
	binary.BigEndian.PutUint32(out[8:12], 2) // rpcvers
	binary.BigEndian.PutUint32(out[12:16], prog)
	binary.BigEndian.PutUint32(out[16:20], vers)
	binary.BigEndian.PutUint32(out[20:24], proc)
	// cred + verf both AUTH_NONE / length 0 (already zero-filled).
	binary.BigEndian.PutUint32(out[40:44], dpLen)
	copy(out[44:44+dpLen], dirpath)
	return out
}

func newMountUDPTestServer(t *testing.T, exportPath string) (*mountUDPServer, *net.UDPConn) {
	t.Helper()
	return newMountUDPTestServerWithClient(t, exportPath, nil)
}

// newMountUDPTestServerWithClient wires Server.withInternalClient when
// client is non-nil, so the under-export lookup branch in handleMount
// can find directory entries.
func newMountUDPTestServerWithClient(t *testing.T, exportPath string, client *fakeNFSFilerClient) (*mountUDPServer, *net.UDPConn) {
	t.Helper()

	exportRoot := normalizeExportRoot(util.FullPath(exportPath))
	authz, err := newClientAuthorizer(nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{
		option:           &Option{},
		exportRoot:       exportRoot,
		exportID:         exportIDForRoot(exportRoot),
		clientAuthorizer: authz,
	}
	if client != nil {
		srv.withInternalClient = func(_ bool, fn func(nfsFilerClient) error) error {
			return fn(client)
		}
	}

	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}

	m := &mountUDPServer{
		bindIP:  "127.0.0.1",
		port:    conn.LocalAddr().(*net.UDPAddr).Port,
		server:  srv,
		udpConn: conn,
		done:    make(chan struct{}),
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.serve()
	}()
	t.Cleanup(func() {
		_ = m.Close()
	})
	return m, conn
}

func sendMountUDP(t *testing.T, target *net.UDPAddr, payload []byte) []byte {
	t.Helper()
	c, err := net.DialUDP("udp", nil, target)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return buf[:n]
}

// parseRPCReply pulls xid, accept_stat, and the body that follows accept_stat
// out of a MSG_ACCEPTED reply. Unlike the TCP path there is no fragment
// marker — the entire UDP datagram is the reply.
func parseRPCReply(t *testing.T, reply []byte) (xid, acceptStat uint32, body []byte) {
	t.Helper()
	if len(reply) < 24 {
		t.Fatalf("reply too short: %d bytes", len(reply))
	}
	xid = binary.BigEndian.Uint32(reply[0:4])
	if mt := binary.BigEndian.Uint32(reply[4:8]); mt != rpcMsgReply {
		t.Fatalf("msg_type=%d want REPLY(1)", mt)
	}
	if rs := binary.BigEndian.Uint32(reply[8:12]); rs != rpcMsgAccepted {
		t.Fatalf("reply_stat=%d want MSG_ACCEPTED(0)", rs)
	}
	acceptStat = binary.BigEndian.Uint32(reply[20:24])
	body = reply[24:]
	return
}

func TestMountUDPNullReturnsSuccess(t *testing.T) {
	m, conn := newMountUDPTestServer(t, "/exports")
	target := conn.LocalAddr().(*net.UDPAddr)

	reply := sendMountUDP(t, target, buildMountCallFrame(7, mountProgram, 3, mountProcNull, ""))
	xid, astat, body := parseRPCReply(t, reply)

	if xid != 7 {
		t.Errorf("xid=%d want 7", xid)
	}
	if astat != rpcAcceptSuccess {
		t.Errorf("accept_stat=%d want SUCCESS(0)", astat)
	}
	if len(body) != 0 {
		t.Errorf("NULL reply body should be empty, got %d bytes", len(body))
	}
	_ = m
}

func TestMountUDPMntReturnsHandleAndFlavors(t *testing.T) {
	m, conn := newMountUDPTestServer(t, "/exports")
	target := conn.LocalAddr().(*net.UDPAddr)

	reply := sendMountUDP(t, target, buildMountCallFrame(42, mountProgram, 3, mountProcMnt, "/exports"))
	xid, astat, body := parseRPCReply(t, reply)

	if xid != 42 {
		t.Errorf("xid=%d want 42", xid)
	}
	if astat != rpcAcceptSuccess {
		t.Fatalf("accept_stat=%d want SUCCESS(0)", astat)
	}
	if len(body) < 4 {
		t.Fatalf("body too short: %d bytes", len(body))
	}
	status := binary.BigEndian.Uint32(body[0:4])
	if status != mnt3StatOK {
		t.Fatalf("mountstat3=%d want OK(0)", status)
	}

	// fhandle3: uint32 length + padded opaque bytes.
	if len(body) < 8 {
		t.Fatalf("body missing handle length: %d bytes", len(body))
	}
	handleLen := binary.BigEndian.Uint32(body[4:8])
	handlePadded := (handleLen + 3) &^ 3
	if uint32(len(body)) < 8+handlePadded+4 {
		t.Fatalf("body truncated: have %d, need at least %d", len(body), 8+handlePadded+4)
	}
	handle := body[8 : 8+handleLen]
	if _, err := DecodeFileHandle(handle); err != nil {
		t.Fatalf("returned handle does not decode: %v", err)
	}

	flavorOff := 8 + handlePadded
	count := binary.BigEndian.Uint32(body[flavorOff : flavorOff+4])
	if count != 2 {
		t.Errorf("flavor count=%d want 2 (NULL + UNIX)", count)
	}
	got := []uint32{
		binary.BigEndian.Uint32(body[flavorOff+4 : flavorOff+8]),
		binary.BigEndian.Uint32(body[flavorOff+8 : flavorOff+12]),
	}
	if got[0] != authFlavorNull || got[1] != authFlavorUnix {
		t.Errorf("flavors=%v want [%d %d]", got, authFlavorNull, authFlavorUnix)
	}
	_ = m
}

func TestMountUDPMntAcceptsAnyPath(t *testing.T) {
	const exportRoot = "/buckets/data"

	_, conn := newMountUDPTestServer(t, exportRoot)
	target := conn.LocalAddr().(*net.UDPAddr)

	dirpaths := []string{
		"/",
		"/buckets",
		"/buckets/other",
		"/wrong/path",
		"",
		"buckets/data",
		exportRoot,
		exportRoot + "/",
	}
	for i, dirpath := range dirpaths {
		t.Run(dirpath, func(t *testing.T) {
			xid := uint32(1000 + i)
			reply := sendMountUDP(t, target, buildMountCallFrame(xid, mountProgram, 3, mountProcMnt, dirpath))
			_, astat, body := parseRPCReply(t, reply)
			if astat != rpcAcceptSuccess {
				t.Fatalf("accept_stat=%d want SUCCESS(0)", astat)
			}
			if len(body) < 4 {
				t.Fatalf("body too short: %d bytes", len(body))
			}
			if got := binary.BigEndian.Uint32(body[0:4]); got != mnt3StatOK {
				t.Errorf("MNT(%q): mountstat3=%d want OK(0)", dirpath, got)
			}
			if len(body) <= 4 {
				t.Errorf("MNT(%q) success body must include handle and flavors", dirpath)
			}
		})
	}
}

func TestMountUDPSubexportMount(t *testing.T) {
	const exportRoot = "/buckets"

	client := &fakeNFSFilerClient{
		entries: map[util.FullPath]*filer_pb.Entry{
			"/buckets":             testEntry("buckets", true, 100, uint32(0755), nil),
			"/buckets/data":        testEntry("data", true, 101, uint32(0755), nil),
			"/buckets/data/nested": testEntry("nested", true, 102, uint32(0755), nil),
			"/buckets/file.txt":    testEntry("file.txt", false, 103, uint32(0644), []byte("hi")),
		},
		kv: map[string][]byte{
			string(filer.InodeIndexKey(100)): testIndexRecord(t, 100, 1, "/buckets"),
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/buckets/data"),
			string(filer.InodeIndexKey(102)): testIndexRecord(t, 102, 1, "/buckets/data/nested"),
			string(filer.InodeIndexKey(103)): testIndexRecord(t, 103, 1, "/buckets/file.txt"),
		},
	}

	m, conn := newMountUDPTestServerWithClient(t, exportRoot, client)
	target := conn.LocalAddr().(*net.UDPAddr)

	// Build a TCP Handler from the same Server so we can compare the
	// raw FH bytes both transports produce for the same subdirectory.
	tcpHandler, err := m.server.newHandler()
	require.NoError(t, err)

	cases := []struct {
		name       string
		dirpath    string
		wantStatus uint32
		wantInode  uint64
	}{
		{name: "subdirectory_one_level", dirpath: "/buckets/data", wantStatus: mnt3StatOK, wantInode: 101},
		{name: "subdirectory_two_levels", dirpath: "/buckets/data/nested", wantStatus: mnt3StatOK, wantInode: 102},
		{name: "subdirectory_trailing_slash", dirpath: "/buckets/data/", wantStatus: mnt3StatOK, wantInode: 101},
		{name: "missing_under_export", dirpath: "/buckets/missing", wantStatus: mnt3ErrNoEnt},
		{name: "deep_missing_under_export", dirpath: "/buckets/data/no-such-thing", wantStatus: mnt3ErrNoEnt},
		{name: "regular_file_not_directory", dirpath: "/buckets/file.txt", wantStatus: mnt3ErrNotDir},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			xid := uint32(2000 + i)
			reply := sendMountUDP(t, target, buildMountCallFrame(xid, mountProgram, 3, mountProcMnt, tc.dirpath))
			_, astat, body := parseRPCReply(t, reply)
			if astat != rpcAcceptSuccess {
				t.Fatalf("accept_stat=%d want SUCCESS(0)", astat)
			}
			if len(body) < 4 {
				t.Fatalf("body too short: %d bytes", len(body))
			}
			got := binary.BigEndian.Uint32(body[0:4])
			if got != tc.wantStatus {
				t.Fatalf("MNT(%q) status=%d want %d", tc.dirpath, got, tc.wantStatus)
			}
			if tc.wantStatus != mnt3StatOK {
				if len(body) != 4 {
					t.Errorf("MNT(%q) error body should carry only the status; got %d trailing bytes", tc.dirpath, len(body)-4)
				}
				return
			}
			if len(body) < 8 {
				t.Fatalf("MNT(%q) success body missing handle length", tc.dirpath)
			}
			handleLen := binary.BigEndian.Uint32(body[4:8])
			if uint32(len(body)) < 8+handleLen {
				t.Fatalf("MNT(%q) success body truncated", tc.dirpath)
			}
			udpHandleBytes := body[8 : 8+handleLen]
			handle, err := DecodeFileHandle(udpHandleBytes)
			if err != nil {
				t.Fatalf("MNT(%q) handle decode: %v", tc.dirpath, err)
			}
			if handle.Inode != tc.wantInode {
				t.Errorf("MNT(%q) FH inode=%d want %d", tc.dirpath, handle.Inode, tc.wantInode)
			}
			if handle.Kind != FileHandleKindDirectory {
				t.Errorf("MNT(%q) FH kind=%d want directory", tc.dirpath, handle.Kind)
			}

			// Transport parity: drive the TCP Handler with the same dirpath
			// and confirm the bytes go-nfs's onMount would write match the
			// UDP responder's bytes exactly. A regression that drifts the
			// generation, exportID, or kind on one transport would fail here.
			tcpStatus, tcpFS, _ := tcpHandler.Mount(context.Background(), nil, gonfs.MountRequest{Dirpath: []byte(tc.dirpath)})
			require.Equal(t, gonfs.MountStatusOk, tcpStatus, "TCP Mount(%q)", tc.dirpath)
			tcpHandleBytes := tcpHandler.ToHandle(tcpFS, nil)
			require.NotEmpty(t, tcpHandleBytes, "TCP Mount(%q) ToHandle returned empty", tc.dirpath)
			assert.Equal(t, tcpHandleBytes, udpHandleBytes, "TCP/UDP FH bytes diverge for %q", tc.dirpath)
		})
	}
}

func TestMountUDPRejectsWrongVersion(t *testing.T) {
	// Same defence-in-depth as the TCP version filter: don't speak v1/v4
	// MOUNT — return PROG_MISMATCH advertising 3..3 so the client knows
	// to retry with v3.
	_, conn := newMountUDPTestServer(t, "/exports")
	target := conn.LocalAddr().(*net.UDPAddr)

	reply := sendMountUDP(t, target, buildMountCallFrame(1, mountProgram, 4, mountProcNull, ""))
	_, astat, body := parseRPCReply(t, reply)

	if astat != rpcAcceptProgMismatch {
		t.Fatalf("accept_stat=%d want PROG_MISMATCH(2)", astat)
	}
	if len(body) != 8 {
		t.Fatalf("PROG_MISMATCH body=%d bytes want 8", len(body))
	}
	low := binary.BigEndian.Uint32(body[0:4])
	high := binary.BigEndian.Uint32(body[4:8])
	if low != 3 || high != 3 {
		t.Errorf("supported range=(%d,%d) want (3,3)", low, high)
	}
}

func TestMountUDPRejectsWrongProgram(t *testing.T) {
	_, conn := newMountUDPTestServer(t, "/exports")
	target := conn.LocalAddr().(*net.UDPAddr)

	// 100021 is NLM, which we don't run here.
	reply := sendMountUDP(t, target, buildMountCallFrame(1, 100021, 4, mountProcNull, ""))
	_, astat, _ := parseRPCReply(t, reply)
	if astat != rpcAcceptProgUnavail {
		t.Errorf("accept_stat=%d want PROG_UNAVAIL(1)", astat)
	}
}

func TestMountUDPUmntAcknowledges(t *testing.T) {
	_, conn := newMountUDPTestServer(t, "/exports")
	target := conn.LocalAddr().(*net.UDPAddr)

	// UMNT carries a dirpath but the server is stateless and ignores it.
	reply := sendMountUDP(t, target, buildMountCallFrame(8, mountProgram, 3, mountProcUmnt, "/exports"))
	_, astat, body := parseRPCReply(t, reply)
	if astat != rpcAcceptSuccess {
		t.Errorf("accept_stat=%d want SUCCESS(0)", astat)
	}
	if len(body) != 0 {
		t.Errorf("UMNT reply body should be empty, got %d bytes", len(body))
	}
}

func TestMountUDPRejectsTruncatedMntArgs(t *testing.T) {
	_, conn := newMountUDPTestServer(t, "/exports")
	target := conn.LocalAddr().(*net.UDPAddr)

	// Hand-craft an MNT call whose dirpath length field claims 16 bytes
	// but no body follows. Using buildMountCallFrame would also emit a
	// trailing length=0 from the empty-string default; we need exactly
	// "length, no body" so the GARBAGE_ARGS path actually fires.
	frame := make([]byte, 24+16+4) // header + auth + 4-byte length only
	binary.BigEndian.PutUint32(frame[0:4], 1)            // xid
	binary.BigEndian.PutUint32(frame[4:8], rpcMsgCall)   // msg_type
	binary.BigEndian.PutUint32(frame[8:12], 2)           // rpcvers
	binary.BigEndian.PutUint32(frame[12:16], mountProgram)
	binary.BigEndian.PutUint32(frame[16:20], 3) // mount vers
	binary.BigEndian.PutUint32(frame[20:24], mountProcMnt)
	// auth = two AUTH_NONE / length-0 stanzas (already zero from make).
	binary.BigEndian.PutUint32(frame[40:44], 16) // dirpath length=16, no bytes follow
	reply := sendMountUDP(t, target, frame)
	_, astat, _ := parseRPCReply(t, reply)
	if astat != rpcAcceptGarbageArgs {
		t.Errorf("accept_stat=%d want GARBAGE_ARGS(4)", astat)
	}
}

func TestMountUDPCloseStopsServing(t *testing.T) {
	m, conn := newMountUDPTestServer(t, "/exports")
	target := conn.LocalAddr().(*net.UDPAddr)

	// Sanity: NULL works before close.
	_ = sendMountUDP(t, target, buildMountCallFrame(1, mountProgram, 3, mountProcNull, ""))

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After Close the socket is shut, so a fresh send should fail to
	// read a reply within the deadline rather than producing a
	// well-formed response.
	c, err := net.DialUDP("udp", nil, target)
	if err != nil {
		// Some platforms refuse the dial outright after Close — that's
		// also acceptable: the server is gone either way.
		return
	}
	defer c.Close()
	_, _ = c.Write(buildMountCallFrame(2, mountProgram, 3, mountProcNull, ""))
	_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1024)
	if _, err := c.Read(buf); err == nil {
		t.Error("Close should have stopped the responder, but a reply still arrived")
	}
}
