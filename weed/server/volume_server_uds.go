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
	UdsRequestSize      = 24  // fid(16) + version(4) + flags(4)
	UdsResponseSize     = 32  // status(1) + pad(3) + volume_id(4) + offset(8) + length(8) + dat_path_len(2) + reserved(6)
	UdsMaxDatPathLen    = 256 // max .dat file path length
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
// Wire format matches sra-common::uds_proto::LocateRequest (repr(C)):
//   opcode(1) + pad(3) + request_id(4) + fid(16) = 24 bytes
type LocateRequest struct {
	Opcode    uint8
	_pad      [3]byte
	RequestId uint32
	Fid       [16]byte // ASCII fid, null-padded
}

// LocateResponse represents a UDS locate response
type LocateResponse struct {
	Status     uint8
	_pad       [3]byte
	VolumeId   uint32
	Offset     uint64
	Length     uint64
	DatPathLen uint16  // length of .dat file path (follows the 32-byte header)
	_          [6]byte // reserved
	DatPath    string  // variable-length .dat file path (not in wire header)
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

		// Parse request (matches sra-common::uds_proto::LocateRequest repr(C))
		// opcode(1) + pad(3) + request_id(4) + fid(16) = 24 bytes
		var req LocateRequest
		req.Opcode = reqBuf[0]
		req.RequestId = binary.LittleEndian.Uint32(reqBuf[4:8])
		copy(req.Fid[:], reqBuf[8:24])

		// Handle request
		resp := u.handleLocate(&req)

		// Serialize response header (32 bytes)
		respBuf[0] = resp.Status
		respBuf[1] = 0
		respBuf[2] = 0
		respBuf[3] = 0
		binary.LittleEndian.PutUint32(respBuf[4:8], resp.VolumeId)
		binary.LittleEndian.PutUint64(respBuf[8:16], resp.Offset)
		binary.LittleEndian.PutUint64(respBuf[16:24], resp.Length)
		binary.LittleEndian.PutUint16(respBuf[24:26], resp.DatPathLen)
		// reserved bytes 26-32 are zero

		// Write fixed header
		if _, err := conn.Write(respBuf); err != nil {
			glog.V(2).Infof("UDS write error: %v", err)
			return
		}

		// Write variable-length .dat path if present
		if resp.DatPathLen > 0 {
			if _, err := conn.Write([]byte(resp.DatPath)); err != nil {
				glog.V(2).Infof("UDS write dat path error: %v", err)
				return
			}
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

	// Include .dat file path so sidecar knows the exact filename
	// (SeaweedFS uses {collection}_{id}.dat when collection is non-empty)
	datPath := v.DataFileName() + ".dat"
	if len(datPath) <= UdsMaxDatPathLen {
		resp.DatPath = datPath
		resp.DatPathLen = uint16(len(datPath))
	}

	glog.V(3).Infof("UDS: located %s -> vol=%d offset=%d size=%d dat=%s", fid, volumeId, resp.Offset, resp.Length, datPath)
	return resp
}
