package weed_server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// UDS protocol constants
const (
	UdsRequestSize  = 24 // fid(16) + version(4) + flags(4)
	UdsResponseSize = 32 // status(1) + pad(3) + volume_id(4) + offset(8) + length(8) + reserved(8)
)

// UDS status codes
const (
	UdsStatusOk       uint8 = 0
	UdsStatusNotFound uint8 = 1
	UdsStatusError    uint8 = 2
)

// UdsServer handles UDS locate requests for RDMA sidecar integration
type UdsServer struct {
	vs       *VolumeServer
	listener net.Listener
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// LocateRequest represents a UDS locate request
type LocateRequest struct {
	Fid     [16]byte // ASCII fid, null-padded
	Version uint32
	Flags   uint32
}

// LocateResponse represents a UDS locate response
type LocateResponse struct {
	Status   uint8
	_pad     [3]byte
	VolumeId uint32
	Offset   uint64
	Length   uint64
	_        [8]byte // reserved
}

// NewUdsServer creates a new UDS server for the volume server
func NewUdsServer(vs *VolumeServer, socketPath string) (*UdsServer, error) {
	// Remove existing socket file if exists
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove existing socket: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", socketPath, err)
	}

	// Set socket permissions to allow access
	if err := os.Chmod(socketPath, 0666); err != nil {
		listener.Close()
		return nil, fmt.Errorf("failed to chmod socket: %w", err)
	}

	uds := &UdsServer{
		vs:       vs,
		listener: listener,
		stopChan: make(chan struct{}),
	}

	glog.V(0).Infof("UDS server listening on %s", socketPath)
	return uds, nil
}

// Start begins accepting connections
func (u *UdsServer) Start() {
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.acceptLoop()
	}()
}

// Stop gracefully shuts down the UDS server
func (u *UdsServer) Stop() {
	close(u.stopChan)
	u.listener.Close()
	u.wg.Wait()
	glog.V(0).Infoln("UDS server stopped")
}

func (u *UdsServer) acceptLoop() {
	for {
		conn, err := u.listener.Accept()
		if err != nil {
			select {
			case <-u.stopChan:
				return
			default:
				glog.V(0).Infof("UDS accept error: %v", err)
				continue
			}
		}

		u.wg.Add(1)
		go func(c net.Conn) {
			defer u.wg.Done()
			u.handleConnection(c)
		}(conn)
	}
}

func (u *UdsServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reqBuf := make([]byte, UdsRequestSize)
	respBuf := make([]byte, UdsResponseSize)

	for {
		// Read request
		_, err := io.ReadFull(conn, reqBuf)
		if err != nil {
			if err != io.EOF {
				glog.V(2).Infof("UDS read error: %v", err)
			}
			return
		}

		// Parse request
		var req LocateRequest
		copy(req.Fid[:], reqBuf[0:16])
		req.Version = binary.LittleEndian.Uint32(reqBuf[16:20])
		req.Flags = binary.LittleEndian.Uint32(reqBuf[20:24])

		// Handle request
		resp := u.handleLocate(&req)

		// Serialize response
		respBuf[0] = resp.Status
		respBuf[1] = 0
		respBuf[2] = 0
		respBuf[3] = 0
		binary.LittleEndian.PutUint32(respBuf[4:8], resp.VolumeId)
		binary.LittleEndian.PutUint64(respBuf[8:16], resp.Offset)
		binary.LittleEndian.PutUint64(respBuf[16:24], resp.Length)
		// reserved bytes 24-32 are zero

		// Write response
		if _, err := conn.Write(respBuf); err != nil {
			glog.V(2).Infof("UDS write error: %v", err)
			return
		}
	}
}

func (u *UdsServer) handleLocate(req *LocateRequest) *LocateResponse {
	resp := &LocateResponse{}

	// Extract fid string (null-terminated)
	fidBytes := req.Fid[:]
	fidLen := 0
	for i, b := range fidBytes {
		if b == 0 {
			fidLen = i
			break
		}
		fidLen = i + 1
	}
	fid := string(fidBytes[:fidLen])
	fid = strings.TrimSpace(fid)

	if fid == "" {
		resp.Status = UdsStatusError
		return resp
	}

	// Parse fid (format: "volumeId,needleIdCookie" e.g., "3,01637037")
	fileId, err := needle.ParseFileIdFromString(fid)
	if err != nil {
		glog.V(2).Infof("UDS: invalid fid %q: %v", fid, err)
		resp.Status = UdsStatusError
		return resp
	}

	if u.vs == nil {
		resp.Status = UdsStatusError
		return resp
	}

	volumeId := fileId.VolumeId
	needleId := fileId.Key

	// Get volume
	v := u.vs.store.GetVolume(volumeId)
	if v == nil {
		glog.V(2).Infof("UDS: volume %d not found for fid %s", volumeId, fid)
		resp.Status = UdsStatusNotFound
		return resp
	}

	// Lookup needle in the volume's needle map
	// We need to access the needle map through the volume
	nv, ok := v.GetNeedle(needleId)
	if !ok || nv.Offset.IsZero() {
		glog.V(3).Infof("UDS: needle %s not found in volume %d", fid, volumeId)
		resp.Status = UdsStatusNotFound
		return resp
	}

	// Check if deleted
	if nv.Size.IsDeleted() {
		glog.V(3).Infof("UDS: needle %s is deleted", fid)
		resp.Status = UdsStatusNotFound
		return resp
	}

	// Return the needle header offset (not data offset)
	// The reader will parse the header to find DataSize
	resp.Status = UdsStatusOk
	resp.VolumeId = uint32(volumeId)
	resp.Offset = uint64(nv.Offset.ToActualOffset())
	resp.Length = uint64(nv.Size)

	glog.V(3).Infof("UDS: located %s -> vol=%d offset=%d size=%d", fid, volumeId, resp.Offset, resp.Length)
	return resp
}
