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
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// UDS protocol constants
const (
	UdsRequestSize      = 24  // opcode(1) + pad(3) + request_id(4) + fid(16)
	UdsResponseSize     = 32  // status(1) + pad(3) + volume_id(4) + offset(8) + length(8) + dat_path_len(2) + reserved(6)
	UdsMaxDatPathLen    = 256 // max .dat file path length

	// Store request: opcode(1) + pad(3) + request_id(4) + volume_id(4) + needle_version(1) + pad(3) + raw_bytes_len(4) = 20 bytes header
	UdsStoreHeaderSize  = 20
	// Store response: status(1) + pad(3) + request_id(4) = 8 bytes
	UdsStoreResponseSize = 8

	// Maximum raw needle size for Store (256 MB)
	UdsMaxRawBytesLen = 256 * 1024 * 1024
)

// UDS opcodes
const (
	UdsOpcodeLocate uint8 = 0x01
	UdsOpcodeStore  uint8 = 0x02
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

// StoreRequest represents a UDS store request for volume replication.
// Wire format:
//
//	opcode(1) + pad(3) + request_id(4) + volume_id(4) +
//	needle_version(1) + pad(3) + raw_bytes_len(4) = 20 bytes header
//	+ raw_bytes(variable)
type StoreRequest struct {
	Opcode        uint8
	RequestId     uint32
	VolumeId      uint32
	NeedleVersion uint8
	RawBytesLen   uint32
	RawBytes      []byte
}

// StoreResponse represents a UDS store response.
// Wire format: status(1) + pad(3) + request_id(4) = 8 bytes
type StoreResponse struct {
	Status    uint8
	RequestId uint32
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

	// Common prefix: opcode(1) + pad(3) + request_id(4) = 8 bytes
	commonBuf := make([]byte, 8)
	locateRestBuf := make([]byte, UdsRequestSize-8) // remaining 16 bytes for Locate
	locateRespBuf := make([]byte, UdsResponseSize)
	storeRestBuf := make([]byte, UdsStoreHeaderSize-8) // remaining 12 bytes for Store header
	storeRespBuf := make([]byte, UdsStoreResponseSize)

	for {
		// Read common prefix (8 bytes)
		_, err := io.ReadFull(conn, commonBuf)
		if err != nil {
			if err != io.EOF {
				glog.V(2).Infof("UDS read error: %v", err)
			}
			return
		}

		opcode := commonBuf[0]
		requestId := binary.LittleEndian.Uint32(commonBuf[4:8])

		switch opcode {
		case UdsOpcodeLocate:
			// Read remaining 16 bytes (fid)
			if _, err := io.ReadFull(conn, locateRestBuf); err != nil {
				glog.V(2).Infof("UDS read locate body: %v", err)
				return
			}

			var req LocateRequest
			req.Opcode = opcode
			req.RequestId = requestId
			copy(req.Fid[:], locateRestBuf)

			resp := u.handleLocate(&req)

			// Serialize Locate response (32 bytes)
			locateRespBuf[0] = resp.Status
			locateRespBuf[1] = 0
			locateRespBuf[2] = 0
			locateRespBuf[3] = 0
			binary.LittleEndian.PutUint32(locateRespBuf[4:8], resp.VolumeId)
			binary.LittleEndian.PutUint64(locateRespBuf[8:16], resp.Offset)
			binary.LittleEndian.PutUint64(locateRespBuf[16:24], resp.Length)
			binary.LittleEndian.PutUint16(locateRespBuf[24:26], resp.DatPathLen)
			// reserved bytes 26-32 are zero
			for i := 26; i < 32; i++ {
				locateRespBuf[i] = 0
			}

			if _, err := conn.Write(locateRespBuf); err != nil {
				glog.V(2).Infof("UDS write error: %v", err)
				return
			}
			if resp.DatPathLen > 0 {
				if _, err := conn.Write([]byte(resp.DatPath)); err != nil {
					glog.V(2).Infof("UDS write dat path error: %v", err)
					return
				}
			}

		case UdsOpcodeStore:
			// Read remaining 12 bytes of Store header
			if _, err := io.ReadFull(conn, storeRestBuf); err != nil {
				glog.V(2).Infof("UDS read store header: %v", err)
				return
			}

			req := StoreRequest{
				Opcode:        opcode,
				RequestId:     requestId,
				VolumeId:      binary.LittleEndian.Uint32(storeRestBuf[0:4]),
				NeedleVersion: storeRestBuf[4],
				RawBytesLen:   binary.LittleEndian.Uint32(storeRestBuf[8:12]),
			}

			// Validate raw_bytes_len
			if req.RawBytesLen == 0 || req.RawBytesLen > UdsMaxRawBytesLen {
				glog.V(2).Infof("UDS: invalid raw_bytes_len %d", req.RawBytesLen)
				u.writeStoreResponse(conn, storeRespBuf, requestId, UdsStatusError)
				return
			}

			// Read raw needle bytes
			req.RawBytes = make([]byte, req.RawBytesLen)
			if _, err := io.ReadFull(conn, req.RawBytes); err != nil {
				glog.V(2).Infof("UDS read store payload: %v", err)
				return
			}

			resp := u.handleStore(&req)
			if err := u.writeStoreResponse(conn, storeRespBuf, resp.RequestId, resp.Status); err != nil {
				return
			}

		default:
			glog.V(0).Infof("UDS: unknown opcode 0x%02x from connection", opcode)
			return
		}
	}
}

func (u *UdsServer) writeStoreResponse(conn net.Conn, buf []byte, requestId uint32, status uint8) error {
	buf[0] = status
	buf[1] = 0
	buf[2] = 0
	buf[3] = 0
	binary.LittleEndian.PutUint32(buf[4:8], requestId)
	if _, err := conn.Write(buf); err != nil {
		glog.V(2).Infof("UDS write store response error: %v", err)
		return err
	}
	return nil
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

func (u *UdsServer) handleStore(req *StoreRequest) *StoreResponse {
	resp := &StoreResponse{RequestId: req.RequestId}

	if u.vs == nil {
		resp.Status = UdsStatusError
		return resp
	}

	// Validate raw bytes contain at least a needle header (Cookie + NeedleId + Size = 16 bytes)
	if len(req.RawBytes) < types.NeedleHeaderSize {
		glog.V(2).Infof("UDS store: raw_bytes too short (%d < %d)", len(req.RawBytes), types.NeedleHeaderSize)
		resp.Status = UdsStatusError
		return resp
	}

	// Parse needle header to extract needle ID and size
	var n needle.Needle
	n.ParseNeedleHeader(req.RawBytes[:types.NeedleHeaderSize])

	vid := needle.VolumeId(req.VolumeId)

	// Use the store's WriteNeedleBlob to append raw bytes and update the needle map
	if err := u.vs.store.WriteVolumeNeedleBlob(vid, n.Id, req.RawBytes, n.Size); err != nil {
		glog.V(2).Infof("UDS store: write failed for vol=%d needle=%v: %v", req.VolumeId, n.Id, err)
		resp.Status = UdsStatusError
		return resp
	}

	glog.V(3).Infof("UDS: stored vol=%d needle=%v size=%d raw_len=%d", req.VolumeId, n.Id, n.Size, len(req.RawBytes))
	resp.Status = UdsStatusOk
	return resp
}
