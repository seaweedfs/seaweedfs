package filer

import (
	"errors"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// GatewayChunkUploader is the minimum surface the shared chunk-upload helper
// needs from a concrete uploader. It is satisfied by *operation.Uploader and
// can be mocked in tests without pulling in the weed/operation package.
type GatewayChunkUploader interface {
	UploadWithRetry(
		filerClient filer_pb.FilerClient,
		assignRequest *filer_pb.AssignVolumeRequest,
		uploadOption *operation.UploadOption,
		genFileUrlFn func(host, fileId string) string,
		reader io.Reader,
	) (fileId string, uploadResult *operation.UploadResult, err error, data []byte)
}

// GatewayChunkUploadRequest captures the inputs SaveGatewayDataAsChunk needs.
// All fields except Reader and FilerClient are optional — empty values map to
// sensible defaults that match what `weed filer`, `weed mount`, `weed nfs`,
// and the WebDAV gateway produced before this helper was factored out.
type GatewayChunkUploadRequest struct {
	// FilerClient issues the AssignVolume RPC. Must be non-nil.
	FilerClient filer_pb.FilerClient
	// Uploader executes the HTTP upload. When nil, operation.NewUploader()
	// is used so the common case needs no explicit wiring.
	Uploader GatewayChunkUploader
	// Reader supplies the bytes to upload. Must be non-nil; the helper
	// reads it fully and relies on UploadWithRetry to report short reads.
	Reader io.Reader

	// Logical path of the target file on the filer. Used by AssignVolume
	// for placement policies (replication, collection, etc.).
	FullPath string
	// File name used in the upload form; defaults to the last path segment
	// of FullPath when empty.
	Filename string
	// Offset of the data inside the logical file — copied into the
	// returned FileChunk.Offset so the filer's chunk view layer can
	// reconstruct the file correctly.
	Offset int64
	// TsNs is the per-chunk modification timestamp written onto the
	// resulting FileChunk. Typically time.Now().UnixNano().
	TsNs int64

	// Assign-time placement options.
	Collection  string
	Replication string
	TtlSec      int32
	DiskType    string
	DataCenter  string

	// Upload-time options.
	Cipher   bool
	MimeType string
	PairMap  map[string]string

	// VolumeServerAccess selects how the filer generates the chunk URL.
	// Supported values: "direct", "publicUrl", "filerProxy". When set to
	// "filerProxy", FilerHTTPAddress must be non-empty.
	VolumeServerAccess string
	// FilerHTTPAddress is the filer host:port that proxies chunk URLs when
	// VolumeServerAccess == "filerProxy".
	FilerHTTPAddress string
}

// SaveGatewayDataAsChunk uploads the bytes in `req.Reader` as a single chunk
// on a volume server and returns a filer FileChunk describing it. It is the
// shared chunk-upload path for the NFS, WebDAV, and future gateway servers;
// mount has extra per-request caching and a pre-allocated file-id pool that
// still live in weed/mount.
//
// Semantics:
//
//   - AssignVolume is driven by the filer client in `req.FilerClient`.
//   - The chunk URL is built by the standard operation.UploadOption machinery
//     and then optionally rewritten when VolumeServerAccess == "filerProxy".
//   - The returned chunk's Offset is `req.Offset` and its ModifiedTsNs is
//     `req.TsNs`. Callers that append to an existing file are responsible
//     for installing the chunk into the entry's chunk list (typically via a
//     filer UpdateEntry).
func SaveGatewayDataAsChunk(req GatewayChunkUploadRequest) (*filer_pb.FileChunk, error) {
	if req.FilerClient == nil {
		return nil, errors.New("SaveGatewayDataAsChunk: nil filer client")
	}
	if req.Reader == nil {
		return nil, errors.New("SaveGatewayDataAsChunk: nil reader")
	}

	uploader := req.Uploader
	if uploader == nil {
		realUploader, err := operation.NewUploader()
		if err != nil {
			return nil, fmt.Errorf("SaveGatewayDataAsChunk: new uploader: %w", err)
		}
		uploader = realUploader
	}

	filename := req.Filename
	if filename == "" {
		if slash := lastSlashIndex(req.FullPath); slash >= 0 && slash+1 < len(req.FullPath) {
			filename = req.FullPath[slash+1:]
		} else {
			filename = req.FullPath
		}
	}

	uploadOption := &operation.UploadOption{
		Filename:          filename,
		Cipher:            req.Cipher,
		IsInputCompressed: false,
		MimeType:          req.MimeType,
		PairMap:           req.PairMap,
	}

	genFileUrlFn := func(host, fileId string) string {
		if req.VolumeServerAccess == "filerProxy" && req.FilerHTTPAddress != "" {
			return fmt.Sprintf("http://%s/?proxyChunkId=%s", req.FilerHTTPAddress, fileId)
		}
		return fmt.Sprintf("http://%s/%s", host, fileId)
	}

	assignRequest := &filer_pb.AssignVolumeRequest{
		Count:       1,
		Replication: req.Replication,
		Collection:  req.Collection,
		TtlSec:      req.TtlSec,
		DiskType:    req.DiskType,
		DataCenter:  req.DataCenter,
		Path:        req.FullPath,
	}

	fileID, uploadResult, uploadErr, _ := uploader.UploadWithRetry(
		req.FilerClient,
		assignRequest,
		uploadOption,
		genFileUrlFn,
		req.Reader,
	)
	if uploadErr != nil {
		return nil, fmt.Errorf("upload data: %w", uploadErr)
	}
	if uploadResult == nil {
		return nil, errors.New("upload data: missing upload result")
	}
	if uploadResult.Error != "" {
		return nil, fmt.Errorf("upload result: %s", uploadResult.Error)
	}
	return uploadResult.ToPbFileChunk(fileID, req.Offset, req.TsNs), nil
}

// lastSlashIndex returns the index of the last '/' in s, or -1 if none.
// Intentionally local so this file has no new test-only imports.
func lastSlashIndex(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}
