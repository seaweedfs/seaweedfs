package nfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The upstream willscott/go-nfs library only serves the MOUNT protocol over
// TCP. Linux's mount.nfs and the in-kernel NFS client default `mountproto` to
// UDP in many configurations, so against a stock `weed nfs` deployment the
// kernel queries portmap for "MOUNT v3 UDP", gets port=0 ("not registered"),
// and either falls back inconsistently or surfaces EPROTONOSUPPORT
// ("requested NFS version or transport protocol is not supported"). The user
// either has to add `mountproto=tcp` / `mountport=2049` to their mount
// options or guess that their distro happens to fall back to TCP on its own.
//
// This responder closes that gap. It speaks just enough of MOUNT v3 to handle
// MOUNT_NULL / MOUNT_MNT / MOUNT_UMNT over UDP — the only procedures the
// kernel actually invokes during mount setup and teardown — so plain
// `mount -t nfs <host>:<export> /mnt` works without any client-side protocol
// hints. The protocol layout is intentionally identical to the TCP MOUNT
// handler in handler.go's Mount() so the two paths return the same
// filehandle and the same set of auth flavors for the same export.
//
// References: RFC 1813 §5 (NFSv3/MOUNTv3), RFC 5531 (RPC).

const (
	mountUDPMaxRecord = 32 * 1024

	// mountUDPRetryBackoff mirrors portmapRetryBackoff so the two
	// listening goroutines back off identically under host pressure.
	mountUDPRetryBackoff = 50 * time.Millisecond

	// mountUDPLookupTimeout bounds any filer round-trip the UDP MOUNT
	// path makes (export-root existence check, subexport lookup). The
	// UDP serve loop is single-threaded, so a stalled filer call would
	// otherwise block every later MOUNT packet.
	mountUDPLookupTimeout = 5 * time.Second

	mountVersion = 3

	mountProcNull = 0
	mountProcMnt  = 1
	mountProcUmnt = 3

	// MOUNT v3 status codes (mountstat3 in RFC 1813 §5.1.1).
	mnt3StatOK         uint32 = 0
	mnt3ErrAcces       uint32 = 13
	mnt3ErrNoEnt       uint32 = 2
	mnt3ErrNotDir      uint32 = 20
	mnt3ErrServerFault uint32 = 10006

	// XDR opaque length cap for dirpath. RFC 1813 §5.1 limits MNTPATHLEN
	// to 1024; cap a bit higher for headroom and reject anything beyond.
	mountUDPMaxPathLen = 4096

	// AuthFlavor numeric IDs (matches go-nfs and RFC 5531 §8).
	authFlavorNull = 0
	authFlavorUnix = 1
)

// mountUDPServer answers MOUNT v3 RPCs over UDP. It listens on the same port
// the NFS TCP server uses (2049 by default), since that's what we advertise
// via portmap, and shares the parent Server's exportRoot, exportID, and
// client allowlist so the UDP MOUNT path applies the same access policy as
// the TCP path.
type mountUDPServer struct {
	bindIP string
	port   int
	server *Server

	udpConn *net.UDPConn

	mu     sync.Mutex
	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

func newMountUDPServer(bindIP string, port int, server *Server) *mountUDPServer {
	return &mountUDPServer{
		bindIP: bindIP,
		port:   port,
		server: server,
		done:   make(chan struct{}),
	}
}

func (m *mountUDPServer) Start() error {
	addr := net.JoinHostPort(m.bindIP, fmt.Sprintf("%d", m.port))
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("mount udp resolve %s: %w", addr, err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("mount udp listen %s: %w", addr, err)
	}
	m.udpConn = udpConn
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.serve()
	}()
	return nil
}

func (m *mountUDPServer) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.done)
	m.mu.Unlock()
	if m.udpConn != nil {
		_ = m.udpConn.Close()
	}
	m.wg.Wait()
	return nil
}

func (m *mountUDPServer) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mountUDPServer) serve() {
	buf := make([]byte, mountUDPMaxRecord)
	for {
		n, addr, err := m.udpConn.ReadFromUDP(buf)
		if err != nil {
			if m.isClosed() {
				return
			}
			// Transient read failure: log, back off, keep the
			// responder alive — same pattern as portmap UDP.
			glog.V(1).Infof("mount udp read: %v", err)
			select {
			case <-m.done:
				return
			case <-time.After(mountUDPRetryBackoff):
				continue
			}
		}
		// Apply the parent server's client allowlist before we even
		// look at the RPC bytes, mirroring the TCP path's
		// allowlistListener wrapping.
		if m.server != nil && m.server.clientAuthorizer != nil && !m.server.clientAuthorizer.isAllowedAddr(addr) {
			glog.V(1).Infof("mount udp: rejecting unauthorized client %s", addr)
			continue
		}
		reply := m.handleCall(buf[:n], addr)
		if reply == nil {
			continue
		}
		if _, err := m.udpConn.WriteToUDP(reply, addr); err != nil {
			glog.V(1).Infof("mount udp write to %s: %v", addr, err)
		}
	}
}

// handleCall classifies one RPC CALL message and returns the encoded reply,
// or nil if the call is malformed enough to drop silently.
func (m *mountUDPServer) handleCall(callBuf []byte, addr *net.UDPAddr) []byte {
	xid, prog, vers, proc, args, err := parseRPCCall(callBuf)
	if err != nil {
		return nil
	}
	if prog != mountProgram {
		return encodeAcceptedReply(xid, rpcAcceptProgUnavail, nil)
	}
	if vers != mountVersion {
		// Mismatch — advertise the v3..v3 we actually support.
		body := make([]byte, 8)
		binary.BigEndian.PutUint32(body[0:4], mountVersion)
		binary.BigEndian.PutUint32(body[4:8], mountVersion)
		return encodeAcceptedReply(xid, rpcAcceptProgMismatch, body)
	}

	switch proc {
	case mountProcNull:
		return encodeAcceptedReply(xid, rpcAcceptSuccess, nil)
	case mountProcMnt:
		return m.handleMount(xid, args, addr)
	case mountProcUmnt:
		// Stateless server: there's nothing to forget, just acknowledge.
		// The client sends back the dirpath in args; we don't need to
		// validate it here because UMNT has no return data.
		return encodeAcceptedReply(xid, rpcAcceptSuccess, nil)
	default:
		// MOUNT v3 also defines DUMP / EXPORT / UMNTALL but the kernel
		// mount path doesn't invoke them. Returning PROC_UNAVAIL is
		// the protocol-correct response.
		return encodeAcceptedReply(xid, rpcAcceptProcUnavail, nil)
	}
}

// handleMount implements MOUNT v3 MNT. RFC 1813 §5.1.4:
//
//	MOUNT3args  { dirpath3 dirpath; }              // XDR opaque
//	MOUNT3res   { mountstat3 status; if OK { handle, auth_flavors[] } }
//
// Mirrors Handler.resolveMountFilesystem: exact match returns the
// synthetic root handle; under-export resolves to the subdirectory's
// handle; outside-export falls back to the synthetic root.
func (m *mountUDPServer) handleMount(xid uint32, args []byte, addr *net.UDPAddr) []byte {
	if len(args) < 4 {
		return encodeAcceptedReply(xid, rpcAcceptGarbageArgs, nil)
	}
	pathLen := binary.BigEndian.Uint32(args[0:4])
	if pathLen > mountUDPMaxPathLen {
		return encodeAcceptedReply(xid, rpcAcceptGarbageArgs, nil)
	}
	padded := (pathLen + 3) &^ 3
	if uint32(len(args)) < 4+padded {
		return encodeAcceptedReply(xid, rpcAcceptGarbageArgs, nil)
	}
	dirpath := string(args[4 : 4+pathLen])
	requested := normalizeExportRoot(util.FullPath(dirpath))
	flavors := []uint32{authFlavorNull, authFlavorUnix}

	ctx, cancel := context.WithTimeout(context.Background(), mountUDPLookupTimeout)
	defer cancel()

	if requested == m.server.exportRoot {
		if status := m.rootMountStatus(ctx); status != mnt3StatOK {
			return encodeMountStatus(xid, status)
		}
		return encodeMountSuccess(xid, syntheticRootHandle(m.server), flavors)
	}
	if !requested.IsUnder(m.server.exportRoot) {
		glog.V(0).Infof("mount udp: client %s requested %q (outside export %q); serving configured export", addr, dirpath, m.server.exportRoot)
		if status := m.rootMountStatus(ctx); status != mnt3StatOK {
			return encodeMountStatus(xid, status)
		}
		return encodeMountSuccess(xid, syntheticRootHandle(m.server), flavors)
	}
	fh, status := m.resolveSubexportFileHandle(ctx, requested)
	if status != mnt3StatOK {
		return encodeMountStatus(xid, status)
	}
	glog.V(1).Infof("mount udp: client %s requested %q under export %q; mounting at subdirectory", addr, dirpath, m.server.exportRoot)
	return encodeMountSuccess(xid, fh, flavors)
}

// rootMountStatus is the UDP analogue of Handler.lstatExportStatus:
// confirms the configured export root still exists in the filer so the
// transport-OK branches can't hand out a handle pointing at a deleted
// directory. Reuses the Server's shared rootFS instance so we don't
// construct a wrapper per MOUNT request.
func (m *mountUDPServer) rootMountStatus(ctx context.Context) uint32 {
	if m.server.withInternalClient == nil {
		return mnt3StatOK
	}
	switch _, err := m.server.rootFilesystem().fileInfoForVirtualPath(ctx, "/"); {
	case err == nil:
		return mnt3StatOK
	case os.IsNotExist(err):
		return mnt3ErrNoEnt
	default:
		glog.Errorf("mount udp: export root %q lookup failed: %v", m.server.exportRoot, err)
		return mnt3ErrServerFault
	}
}

// resolveSubexportFileHandle is the UDP analogue of the sub-fs branch in
// Handler.resolveMountFilesystem. The TCP path lets go-nfs's onMount call
// ToHandle on the returned filesystem; UDP encodes the FH itself, so the
// inode/generation lookup happens explicitly here.
//
// The UDP listener is up before serve() runs newHandler(), so a subexport
// MOUNT can land here before sharedReaderCache has been assigned. Resolve
// the rootFS first to drive Server.rootFilesystem's sync.Once and read
// the cache directly off it, so the new sub-fs always shares the same
// reader cache the TCP path uses.
func (m *mountUDPServer) resolveSubexportFileHandle(ctx context.Context, requested util.FullPath) ([]byte, uint32) {
	if m.server.withInternalClient == nil {
		return nil, mnt3ErrServerFault
	}
	rootFS := m.server.rootFilesystem()
	subFS := newSeaweedFileSystem(m.server, requested, rootFS.readerCache)
	info, err := subFS.fileInfoForVirtualPath(ctx, "/")
	switch {
	case err == nil:
	case os.IsNotExist(err):
		return nil, mnt3ErrNoEnt
	default:
		glog.Errorf("mount udp: subexport lookup %q failed: %v", requested, err)
		return nil, mnt3ErrServerFault
	}
	if !info.entry.IsDirectory {
		return nil, mnt3ErrNotDir
	}
	inode := info.entry.GetAttributes().GetInode()
	return NewFileHandle(m.server.exportID, FileHandleKindDirectory, inode, info.generation).Encode(), mnt3StatOK
}

func syntheticRootHandle(s *Server) []byte {
	return NewFileHandle(s.exportID, FileHandleKindDirectory, 0, filer.InodeIndexInitialGeneration).Encode()
}

// encodeMountStatus returns a MOUNT MNT reply carrying just an error status.
// Per RFC 1813 §5.1.4 a non-OK status terminates the response — no handle or
// flavors follow.
func encodeMountStatus(xid, status uint32) []byte {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, status)
	return encodeAcceptedReply(xid, rpcAcceptSuccess, body)
}

// encodeMountSuccess builds the OK MOUNT MNT reply: status=OK, file handle
// (XDR opaque), and the supported auth_flavors list.
func encodeMountSuccess(xid uint32, handle []byte, flavors []uint32) []byte {
	handleLen := uint32(len(handle))
	handlePadded := (handleLen + 3) &^ 3
	bodyLen := 4 + 4 + handlePadded + 4 + 4*uint32(len(flavors))

	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint32(body[0:4], mnt3StatOK)
	binary.BigEndian.PutUint32(body[4:8], handleLen)
	copy(body[8:8+handleLen], handle)
	// Trailing pad bytes are already zero from make().

	pos := 8 + handlePadded
	binary.BigEndian.PutUint32(body[pos:pos+4], uint32(len(flavors)))
	pos += 4
	for _, fl := range flavors {
		binary.BigEndian.PutUint32(body[pos:pos+4], fl)
		pos += 4
	}

	return encodeAcceptedReply(xid, rpcAcceptSuccess, body)
}
