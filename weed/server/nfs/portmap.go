package nfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Minimal PORTMAP v2 responder.
//
// The upstream willscott/go-nfs library serves NFSv3 and MOUNT on a single TCP
// port and deliberately does not register with portmap (RPC program 100000).
// Linux mount.nfs, however, queries portmap on port 111 before sending the
// MOUNT RPC, so the plain `mount -t nfs host:/export /mnt` command fails
// against a default `weed nfs` deployment.
//
// When enabled, this responder binds the privileged port 111 (RFC 1833) on
// both TCP and UDP and answers the subset of PORTMAP v2 calls that standard
// Linux clients make: PMAP_NULL, PMAP_GETPORT and PMAP_DUMP. It refuses
// registration from third parties (PMAP_SET / PMAP_UNSET return false) and
// only exposes the programs that weed itself serves.
//
// References: RFC 1833 (Portmap v2), RFC 5531 (RPC).
const (
	portmapProgram = 100000
	portmapVersion = 2
	portmapPort    = 111

	pmapProcNull    = 0
	pmapProcSet     = 1
	pmapProcUnset   = 2
	pmapProcGetPort = 3
	pmapProcDump    = 4

	ipProtoTCP = 6
	ipProtoUDP = 17

	nfsProgram   = 100003
	mountProgram = 100005

	// RPC
	rpcMsgCall  = 0
	rpcMsgReply = 1

	rpcMsgAccepted = 0

	rpcAcceptSuccess      = 0
	rpcAcceptProgUnavail  = 1
	rpcAcceptProgMismatch = 2
	rpcAcceptProcUnavail  = 3
	rpcAcceptGarbageArgs  = 4

	rpcAuthNone = 0

	// Defensive limits. Portmap messages are tiny in practice; these caps
	// protect the responder from large or slow reads.
	portmapMaxRecord = 64 * 1024

	// Per-connection read/write deadlines on the TCP listener. The idle
	// timeout bounds how long we wait for the next request on an otherwise
	// quiet connection; the IO timeout bounds a single read or write once
	// one is in flight. Both guard against slowloris-style stalls on the
	// privileged port 111.
	portmapTCPIdleTimeout = 30 * time.Second
	portmapTCPIOTimeout   = 10 * time.Second

	// Back-off applied before retrying after a non-fatal listener error
	// (e.g. EMFILE on TCP Accept, or a transient UDP read failure) so we
	// don't busy-loop when the host is under pressure.
	portmapRetryBackoff = 50 * time.Millisecond
)

type portmapEntry struct {
	Program  uint32
	Version  uint32
	Protocol uint32
	Port     uint32
}

type portmapServer struct {
	bindIP  string
	port    int
	entries []portmapEntry

	tcpListener net.Listener
	udpConn     *net.UDPConn

	// mu guards closed and conns. It is held only for bookkeeping, never
	// across network IO.
	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
	// done is closed exactly once by Close() so that background loops can
	// interrupt a retry-backoff sleep instead of waiting it out.
	done chan struct{}
	wg   sync.WaitGroup
}

// newPortmapServer builds a responder advertising the NFS services the caller
// runs on nfsTCPPort. We expose NFS v3 TCP and MOUNT v3 TCP only: the
// underlying library does not handle UDP or older MOUNT versions, so it would
// be misleading to advertise them.
func newPortmapServer(bindIP string, port int, nfsTCPPort uint32) *portmapServer {
	if port <= 0 {
		port = portmapPort
	}
	return &portmapServer{
		bindIP: bindIP,
		port:   port,
		done:   make(chan struct{}),
		entries: []portmapEntry{
			{Program: nfsProgram, Version: 3, Protocol: ipProtoTCP, Port: nfsTCPPort},
			{Program: mountProgram, Version: 3, Protocol: ipProtoTCP, Port: nfsTCPPort},
		},
	}
}

func (ps *portmapServer) Start() error {
	addr := net.JoinHostPort(ps.bindIP, fmt.Sprintf("%d", ps.port))

	tcpLn, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("portmap tcp listen %s: %w", addr, err)
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		_ = tcpLn.Close()
		return fmt.Errorf("portmap udp resolve %s: %w", addr, err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		_ = tcpLn.Close()
		return fmt.Errorf("portmap udp listen %s: %w", addr, err)
	}
	ps.tcpListener = tcpLn
	ps.udpConn = udpConn

	ps.wg.Add(2)
	go func() {
		defer ps.wg.Done()
		ps.serveTCP()
	}()
	go func() {
		defer ps.wg.Done()
		ps.serveUDP()
	}()
	return nil
}

func (ps *portmapServer) Close() error {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil
	}
	ps.closed = true
	conns := ps.conns
	ps.conns = nil
	close(ps.done)
	ps.mu.Unlock()

	var first error
	if ps.tcpListener != nil {
		if err := ps.tcpListener.Close(); err != nil {
			first = err
		}
	}
	if ps.udpConn != nil {
		if err := ps.udpConn.Close(); err != nil && first == nil {
			first = err
		}
	}
	// Evict in-flight TCP handlers so Close() does not block on idle
	// clients; their read goroutines will unwind on the closed conn.
	for c := range conns {
		_ = c.Close()
	}
	ps.wg.Wait()
	return first
}

func (ps *portmapServer) isClosed() bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.closed
}

// addConn registers c for shutdown eviction. It returns false (and the
// caller must drop c) if the server has already started shutting down.
func (ps *portmapServer) addConn(c net.Conn) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return false
	}
	if ps.conns == nil {
		ps.conns = make(map[net.Conn]struct{})
	}
	ps.conns[c] = struct{}{}
	return true
}

func (ps *portmapServer) removeConn(c net.Conn) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.conns, c)
}

func (ps *portmapServer) serveTCP() {
	for {
		conn, err := ps.tcpListener.Accept()
		if err != nil {
			if ps.isClosed() {
				return
			}
			// Non-fatal (e.g. EMFILE, EINTR): log and back off rather
			// than tear the listener down on a transient resource blip.
			// Wake early if Close() fires during the sleep.
			glog.V(1).Infof("portmap tcp accept: %v", err)
			select {
			case <-ps.done:
				return
			case <-time.After(portmapRetryBackoff):
				continue
			}
		}
		if !ps.addConn(conn) {
			_ = conn.Close()
			continue
		}
		ps.wg.Add(1)
		go func(c net.Conn) {
			defer ps.wg.Done()
			defer ps.removeConn(c)
			ps.handleTCPConn(c)
		}(conn)
	}
}

func (ps *portmapServer) handleTCPConn(conn net.Conn) {
	defer conn.Close()
	hdr := make([]byte, 4)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(portmapTCPIdleTimeout))
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return
		}
		mark := binary.BigEndian.Uint32(hdr)
		// Bit 31: last-fragment flag. Portmap messages are always single
		// fragment in practice; drop the connection if we see otherwise.
		if mark&(1<<31) == 0 {
			return
		}
		recLen := mark &^ (1 << 31)
		if recLen == 0 || recLen > portmapMaxRecord {
			return
		}
		buf := make([]byte, recLen)
		_ = conn.SetReadDeadline(time.Now().Add(portmapTCPIOTimeout))
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		reply := ps.handleCall(buf)
		if reply == nil {
			continue
		}
		out := make([]byte, 4+len(reply))
		binary.BigEndian.PutUint32(out[0:4], uint32(len(reply))|(1<<31))
		copy(out[4:], reply)
		_ = conn.SetWriteDeadline(time.Now().Add(portmapTCPIOTimeout))
		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

func (ps *portmapServer) serveUDP() {
	buf := make([]byte, portmapMaxRecord)
	for {
		n, addr, err := ps.udpConn.ReadFromUDP(buf)
		if err != nil {
			if ps.isClosed() {
				return
			}
			// Transient read failure: log, back off, and keep the
			// responder alive instead of taking UDP portmap down.
			// Wake early if Close() fires during the sleep.
			glog.V(1).Infof("portmap udp read: %v", err)
			select {
			case <-ps.done:
				return
			case <-time.After(portmapRetryBackoff):
				continue
			}
		}
		reply := ps.handleCall(buf[:n])
		if reply == nil {
			continue
		}
		if _, err := ps.udpConn.WriteToUDP(reply, addr); err != nil {
			glog.V(1).Infof("portmap udp write to %s: %v", addr, err)
		}
	}
}

// handleCall parses one RPC CALL message and returns the encoded reply, or nil
// if the call is malformed enough that we should drop it silently.
func (ps *portmapServer) handleCall(callBuf []byte) []byte {
	xid, prog, vers, proc, args, err := parseRPCCall(callBuf)
	if err != nil {
		return nil
	}
	if prog != portmapProgram {
		return encodeAcceptedReply(xid, rpcAcceptProgUnavail, nil)
	}
	if vers != portmapVersion {
		// Program-version mismatch: RFC 5531 says we should return the
		// accepted range; keep it simple and report 2..2.
		body := make([]byte, 8)
		binary.BigEndian.PutUint32(body[0:4], portmapVersion)
		binary.BigEndian.PutUint32(body[4:8], portmapVersion)
		return encodeAcceptedReply(xid, rpcAcceptProgMismatch, body)
	}
	switch proc {
	case pmapProcNull:
		return encodeAcceptedReply(xid, rpcAcceptSuccess, nil)
	case pmapProcGetPort:
		if len(args) < 16 {
			return encodeAcceptedReply(xid, rpcAcceptGarbageArgs, nil)
		}
		q := portmapEntry{
			Program:  binary.BigEndian.Uint32(args[0:4]),
			Version:  binary.BigEndian.Uint32(args[4:8]),
			Protocol: binary.BigEndian.Uint32(args[8:12]),
		}
		port := uint32(0)
		for _, e := range ps.entries {
			if e.Program == q.Program && e.Version == q.Version && e.Protocol == q.Protocol {
				port = e.Port
				break
			}
		}
		body := make([]byte, 4)
		binary.BigEndian.PutUint32(body, port)
		return encodeAcceptedReply(xid, rpcAcceptSuccess, body)
	case pmapProcDump:
		// Each entry is 4-byte value_follows + 16-byte mapping = 20 bytes,
		// plus a 4-byte terminator value_follows=FALSE.
		body := make([]byte, 0, 20*len(ps.entries)+4)
		for _, e := range ps.entries {
			chunk := make([]byte, 20)
			binary.BigEndian.PutUint32(chunk[0:4], 1) // value_follows = TRUE
			binary.BigEndian.PutUint32(chunk[4:8], e.Program)
			binary.BigEndian.PutUint32(chunk[8:12], e.Version)
			binary.BigEndian.PutUint32(chunk[12:16], e.Protocol)
			binary.BigEndian.PutUint32(chunk[16:20], e.Port)
			body = append(body, chunk...)
		}
		end := make([]byte, 4) // value_follows = FALSE
		body = append(body, end...)
		return encodeAcceptedReply(xid, rpcAcceptSuccess, body)
	case pmapProcSet, pmapProcUnset:
		// Don't accept third-party registrations. bool=FALSE.
		body := make([]byte, 4)
		return encodeAcceptedReply(xid, rpcAcceptSuccess, body)
	default:
		return encodeAcceptedReply(xid, rpcAcceptProcUnavail, nil)
	}
}

// parseRPCCall parses the fixed portion of an RPC CALL header and returns the
// remaining procedure arguments. It skips both opaque_auth fields (cred and
// verf) so callers get a buffer starting at the procedure arguments.
func parseRPCCall(buf []byte) (xid, prog, vers, proc uint32, args []byte, err error) {
	// Minimum header: xid + msg_type + rpcvers + prog + vers + proc + 2x
	// (flavor + len) = 6*4 + 2*8 = 40 bytes.
	const minHeader = 40
	if len(buf) < minHeader {
		err = fmt.Errorf("rpc call too short: %d bytes", len(buf))
		return
	}
	xid = binary.BigEndian.Uint32(buf[0:4])
	if msgType := binary.BigEndian.Uint32(buf[4:8]); msgType != rpcMsgCall {
		err = fmt.Errorf("not an rpc call: msg_type=%d", msgType)
		return
	}
	if rpcvers := binary.BigEndian.Uint32(buf[8:12]); rpcvers != 2 {
		err = fmt.Errorf("unsupported rpc version %d", rpcvers)
		return
	}
	prog = binary.BigEndian.Uint32(buf[12:16])
	vers = binary.BigEndian.Uint32(buf[16:20])
	proc = binary.BigEndian.Uint32(buf[20:24])

	p := 24
	for i := 0; i < 2; i++ {
		if len(buf) < p+8 {
			err = fmt.Errorf("truncated opaque_auth at offset %d", p)
			return
		}
		authLen := binary.BigEndian.Uint32(buf[p+4 : p+8])
		// Validate before applying the XDR 4-byte padding so that
		// lengths near uint32 max can't wrap to a tiny padded value.
		if authLen > uint32(portmapMaxRecord) {
			err = errors.New("opaque_auth length exceeds limit")
			return
		}
		padded := (authLen + 3) &^ 3
		end := uint64(p) + 8 + uint64(padded)
		if end > uint64(len(buf)) {
			err = fmt.Errorf("truncated opaque_auth body at offset %d (len=%d)", p, authLen)
			return
		}
		p = int(end)
	}
	args = buf[p:]
	return
}

// encodeAcceptedReply builds a MSG_ACCEPTED reply with the given accept_stat.
// body is the already-XDR-encoded data that follows accept_stat in the reply.
// For SUCCESS it is the procedure result; it is nil for most error
// accept_stat values (PROG_UNAVAIL, PROC_UNAVAIL, GARBAGE_ARGS) but is
// non-nil for PROG_MISMATCH, which carries a struct { uint32 low; uint32
// high; } mismatch_info range per RFC 5531 §9.
func encodeAcceptedReply(xid, acceptStat uint32, body []byte) []byte {
	out := make([]byte, 24+len(body))
	binary.BigEndian.PutUint32(out[0:4], xid)
	binary.BigEndian.PutUint32(out[4:8], rpcMsgReply)
	binary.BigEndian.PutUint32(out[8:12], rpcMsgAccepted)
	// verf: AUTH_NONE, zero-length opaque
	binary.BigEndian.PutUint32(out[12:16], rpcAuthNone)
	binary.BigEndian.PutUint32(out[16:20], 0)
	binary.BigEndian.PutUint32(out[20:24], acceptStat)
	copy(out[24:], body)
	return out
}
