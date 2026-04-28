package nfs

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// The upstream willscott/go-nfs library dispatches RPC calls by (program,
// procedure) only — it does not validate the RPC program version. That means
// a Linux client speaking NFSv4 (program 100003 vers 4) lands on the same
// handler map as NFSv3: proc=1 routes to NFSv3 SETATTR, which parses the
// NFSv4 COMPOUND args as if they were SETATTR3args and writes a malformed
// reply. The client cannot decode that reply, the kernel returns
// EPROTONOSUPPORT, and mount.nfs prints "requested NFS version or transport
// protocol is not supported" without ever falling back to v3.
//
// The default Linux mount.nfs path is to try NFSv4 first, so this affects
// every plain `mount -t nfs <host>:<export> /mnt` against a `weed nfs`
// deployment. To make the v4→v3 fallback work, we wrap the listener so the
// first RPC frame on each new TCP connection is inspected: if the program is
// NFS or MOUNT and the version is not 3, we synthesize a PROG_MISMATCH reply
// (with the supported version range 3..3) directly to the socket and close
// the connection. The client then retries with v3 and proceeds normally.
//
// Clients keep the same program/version for the lifetime of a TCP connection
// in practice, so we only need to check the first frame; subsequent frames
// flow through to go-nfs unchanged. This avoids vendoring go-nfs while still
// producing protocol-correct rejections.

// RPC numeric constants used here (rpcMsgCall, rpcMsgReply, rpcMsgAccepted,
// rpcAcceptProgMismatch, rpcAuthNone, nfsProgram, mountProgram) are defined
// alongside the portmap responder in portmap.go to keep one source of truth
// per package.
const (
	// rpcVersionFilterPeekTimeout bounds how long we wait for the first frame
	// header on a new connection before giving up and letting go-nfs handle
	// the (possibly half-open) socket.
	rpcVersionFilterPeekTimeout = 10 * time.Second

	// peeked length: 4-byte fragment marker + 24 bytes of fixed RPC header
	// (xid + msg_type + rpcvers + prog + vers + proc).
	rpcVersionFilterPeekLen = 28

	supportedNFSVer = 3
)

type versionFilterListener struct {
	net.Listener
}

func newVersionFilterListener(inner net.Listener) net.Listener {
	return &versionFilterListener{Listener: inner}
}

func (l *versionFilterListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		wrapped, accepted := filterFirstRPCFrame(conn)
		if !accepted {
			// Already replied with PROG_MISMATCH and closed.
			continue
		}
		return wrapped, nil
	}
}

// peekedConn returns the bytes that filterFirstRPCFrame already buffered when
// it peeked the first RPC header, then transparently reads from the
// underlying connection. Writes go straight to the socket; the bufio reader
// only buffers the read side.
type peekedConn struct {
	net.Conn
	reader io.Reader
}

func (c *peekedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// filterFirstRPCFrame inspects the first RPC frame on conn and decides whether
// to pass it through to go-nfs. Returns (wrappedConn, true) if the frame is
// for a supported (program, version) — including programs we don't recognize,
// since go-nfs handles its own PROG_UNAVAIL response. Returns (nil, false) if
// we already replied with PROG_MISMATCH and closed conn.
//
// On peek failure (early close, deadline) we pass the connection through:
// returning an error here would silently drop legitimate clients on a flaky
// link, and go-nfs has its own per-frame error handling.
func filterFirstRPCFrame(conn net.Conn) (net.Conn, bool) {
	r := bufio.NewReader(conn)

	deadlineErr := conn.SetReadDeadline(time.Now().Add(rpcVersionFilterPeekTimeout))

	hdr, peekErr := r.Peek(rpcVersionFilterPeekLen)

	// Always clear the deadline before returning to go-nfs; failing to do so
	// would make every subsequent Read() time out at the same instant.
	if deadlineErr == nil {
		_ = conn.SetReadDeadline(time.Time{})
	}

	if peekErr != nil {
		return &peekedConn{Conn: conn, reader: r}, true
	}

	fragMark := binary.BigEndian.Uint32(hdr[0:4])
	if fragMark&(1<<31) == 0 {
		// Multi-fragment record: portmap-style filtering of the first frame
		// would need reassembly. Fall through to go-nfs which handles this.
		return &peekedConn{Conn: conn, reader: r}, true
	}

	xid := binary.BigEndian.Uint32(hdr[4:8])
	if msgType := binary.BigEndian.Uint32(hdr[8:12]); msgType != rpcMsgCall {
		// Not a CALL — odd, but pass through.
		return &peekedConn{Conn: conn, reader: r}, true
	}

	prog := binary.BigEndian.Uint32(hdr[16:20])
	vers := binary.BigEndian.Uint32(hdr[20:24])

	switch prog {
	case nfsProgram, mountProgram:
	default:
		// Unknown program: let go-nfs reply PROG_UNAVAIL itself.
		return &peekedConn{Conn: conn, reader: r}, true
	}

	if vers == supportedNFSVer {
		return &peekedConn{Conn: conn, reader: r}, true
	}

	glog.V(1).Infof("nfs: rejecting client %s with PROG_MISMATCH: prog=%d vers=%d (supported=%d)",
		conn.RemoteAddr(), prog, vers, supportedNFSVer)

	if err := writeProgMismatchTCP(conn, xid, supportedNFSVer, supportedNFSVer); err != nil {
		glog.V(1).Infof("nfs: write PROG_MISMATCH to %s: %v", conn.RemoteAddr(), err)
	}
	_ = conn.Close()
	return nil, false
}

// writeProgMismatchTCP encodes a single-frame TCP RPC reply carrying
// MSG_ACCEPTED + PROG_MISMATCH along with the supported version range, per
// RFC 5531 section 9. The frame layout is:
//
//	uint32 fragment_header (last-fragment | length)
//	uint32 xid
//	uint32 msg_type=REPLY(1)
//	uint32 reply_stat=MSG_ACCEPTED(0)
//	uint32 verf_flavor=AUTH_NONE(0)
//	uint32 verf_len=0
//	uint32 accept_stat=PROG_MISMATCH(2)
//	uint32 low
//	uint32 high
const progMismatchBodyLen = 32

func writeProgMismatchTCP(w io.Writer, xid, low, high uint32) error {
	out := make([]byte, 4+progMismatchBodyLen)
	binary.BigEndian.PutUint32(out[0:4], uint32(progMismatchBodyLen)|(1<<31))
	binary.BigEndian.PutUint32(out[4:8], xid)
	binary.BigEndian.PutUint32(out[8:12], rpcMsgReply)
	binary.BigEndian.PutUint32(out[12:16], rpcMsgAccepted)
	binary.BigEndian.PutUint32(out[16:20], rpcAuthNone)
	binary.BigEndian.PutUint32(out[20:24], 0) // verf opaque length (always zero for AUTH_NONE)
	binary.BigEndian.PutUint32(out[24:28], rpcAcceptProgMismatch)
	binary.BigEndian.PutUint32(out[28:32], low)
	binary.BigEndian.PutUint32(out[32:36], high)
	_, err := w.Write(out)
	return err
}
