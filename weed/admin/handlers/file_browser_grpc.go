package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

// Admin upload chunk size, matching s3api so files split into the same fid-sized pieces.
const adminUploadChunkSize = 8 * 1024 * 1024

// File browser handlers backed by the filer gRPC service. They bypass the
// filer's HTTP listener so the UI keeps working when the filer is started
// with -disableHttp=true; chunk bytes still flow through the volume server
// HTTP endpoints (which run on their own ports).

// fetchFileContentGrpc reads file content via the filer gRPC service, looking
// the entry up and then streaming the chunks straight from the volume servers.
// When maxBytes > 0 the stream is truncated to that many bytes — used by the
// "is this text?" sniff so unknown-MIME files don't get fully downloaded.
func (h *FileBrowserHandlers) fetchFileContentGrpc(ctx context.Context, filePath string, maxBytes int) (string, error) {
	cleanFilePath, err := h.validateAndCleanFilePath(filePath)
	if err != nil {
		return "", err
	}

	entry, err := h.lookupEntry(ctx, cleanFilePath)
	if err != nil {
		return "", err
	}
	if entry.IsDirectory {
		return "", fmt.Errorf("path is a directory")
	}

	size := int64(filer.FileSize(entry))
	streamSize := size
	if maxBytes > 0 && streamSize > int64(maxBytes) {
		streamSize = int64(maxBytes)
	}

	var buf bytes.Buffer
	if err := h.streamEntryContent(ctx, entry, streamSize, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// downloadFileGrpc streams a file via gRPC + volume server HTTP. The
// response writer receives the canonical attachment headers and the raw
// bytes; this replaces the HTTP-to-filer proxy that used to run in
// DownloadFile.
func (h *FileBrowserHandlers) downloadFileGrpc(ctx context.Context, filePath string, w http.ResponseWriter, inline bool) error {
	cleanFilePath, err := h.validateAndCleanFilePath(filePath)
	if err != nil {
		return err
	}

	entry, err := h.lookupEntry(ctx, cleanFilePath)
	if err != nil {
		return err
	}
	if entry.IsDirectory {
		return fmt.Errorf("path is a directory")
	}

	size := int64(filer.FileSize(entry))

	fileName := path.Base(cleanFilePath)

	// Resolve mime like the viewer does so Content-Type and the inline check agree.
	contentType := dash.ResolveEntryMime(entry)

	// Only inline images and PDFs; serve the rest as attachments so a hostile upload
	// (HTML, SVG) can't run as same-origin script. nosniff locks the declared type.
	disposition := "attachment"
	if inline && (strings.HasPrefix(contentType, "image/") || contentType == "application/pdf") {
		disposition = "inline"
	}
	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": fileName}))
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)

	return h.streamEntryContent(ctx, entry, size, w)
}

// uploadFileGrpc streams the upload to volumes in 8 MiB chunks via the shared
// chunked-upload helper, then registers the entry over the filer gRPC service.
// Content always lands in volumes, never inlined on the entry.
func (h *FileBrowserHandlers) uploadFileGrpc(ctx context.Context, filePath string, fileName string, mimeType string, reader io.Reader) error {
	cleanFilePath, err := h.validateAndCleanFilePath(filePath)
	if err != nil {
		return err
	}
	dir := path.Dir(cleanFilePath)
	if dir == "." {
		dir = "/"
	}
	entryName := path.Base(cleanFilePath)
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	assignFunc := func(ctx context.Context, count int, expectedDataSize uint64) (*operation.VolumeAssignRequest, *operation.AssignResult, error) {
		var assignResp *filer_pb.AssignVolumeResponse
		err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			resp, assignErr := client.AssignVolume(ctx, &filer_pb.AssignVolumeRequest{
				Count:            int32(count),
				Path:             cleanFilePath,
				ExpectedDataSize: expectedDataSize,
			})
			if assignErr != nil {
				return assignErr
			}
			if resp.Error != "" {
				return fmt.Errorf("%s", resp.Error)
			}
			assignResp = resp
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		if assignResp.Location == nil || assignResp.FileId == "" {
			return nil, nil, fmt.Errorf("assign volume returned empty location")
		}
		return nil, &operation.AssignResult{
			Fid:       assignResp.FileId,
			Url:       assignResp.Location.Url,
			PublicUrl: assignResp.Location.PublicUrl,
			Count:     uint64(count),
			Auth:      security.EncodedJwt(assignResp.Auth),
		}, nil
	}

	chunkResult, err := operation.UploadReaderInChunks(ctx, reader, &operation.ChunkedUploadOption{
		ChunkSize:  adminUploadChunkSize,
		MimeType:   mimeType,
		AssignFunc: assignFunc,
	})
	if err != nil {
		// Partial chunks come back even on error so we can clean them up rather
		// than leaving orphaned data on volume servers.
		if chunkResult != nil && len(chunkResult.FileChunks) > 0 {
			h.deleteOrphanedChunks(chunkResult.FileChunks)
		}
		return fmt.Errorf("upload: %w", err)
	}

	now := time.Now()
	entry := &filer_pb.Entry{
		Name: entryName,
		Attributes: &filer_pb.FuseAttributes{
			FileSize: uint64(chunkResult.TotalSize),
			Mtime:    now.Unix(),
			Crtime:   now.Unix(),
			FileMode: 0644,
			Mime:     mimeType,
		},
	}
	entry.Chunks = chunkResult.FileChunks

	err = h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, createErr := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
		return createErr
	})
	if err != nil {
		if len(chunkResult.FileChunks) > 0 {
			h.deleteOrphanedChunks(chunkResult.FileChunks)
		}
		return fmt.Errorf("create entry: %w", err)
	}
	return nil
}

// deleteOrphanedChunks best-effort removes the chunk fids when an upload
// fails partway through. Errors are logged; we can't surface them past the
// caller's primary failure.
func (h *FileBrowserHandlers) deleteOrphanedChunks(chunks []*filer_pb.FileChunk) {
	fileIds := make([]string, 0, len(chunks))
	for _, c := range chunks {
		if fid := c.GetFileIdString(); fid != "" {
			fileIds = append(fileIds, fid)
		}
	}
	if len(fileIds) == 0 {
		return
	}
	master := h.adminServer.GetMasterClient()
	results := operation.DeleteFileIds(master.GetMaster, false, h.adminServer.GetGrpcDialOption(), fileIds)
	for _, r := range results {
		if r.Error != "" {
			glog.Warningf("admin file browser: orphan chunk %s cleanup: %s", r.FileId, r.Error)
		}
	}
}

func (h *FileBrowserHandlers) lookupEntry(ctx context.Context, cleanFilePath string) (*filer_pb.Entry, error) {
	dir := path.Dir(cleanFilePath)
	if dir == "." {
		dir = "/"
	}
	name := path.Base(cleanFilePath)
	var entry *filer_pb.Entry
	err := h.adminServer.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, lookupErr := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr != nil {
			return lookupErr
		}
		if resp.Entry == nil {
			return fmt.Errorf("not found")
		}
		entry = resp.Entry
		return nil
	})
	return entry, err
}

func (h *FileBrowserHandlers) streamEntryContent(ctx context.Context, entry *filer_pb.Entry, size int64, w io.Writer) error {
	if size == 0 {
		// Inline content (small files stored directly on the entry) skip the
		// chunk pipeline entirely.
		if len(entry.Content) > 0 {
			_, err := w.Write(entry.Content)
			return err
		}
		return nil
	}
	if len(entry.Content) > 0 && len(entry.GetChunks()) == 0 {
		_, err := w.Write(entry.Content)
		return err
	}

	streamFn, err := filer.PrepareStreamContentWithThrottler(
		ctx,
		h.adminServer.GetMasterClient(),
		dash.VolumeServerReadJwt,
		entry.GetChunks(),
		0,
		size,
		0,
	)
	if err != nil {
		return fmt.Errorf("prepare stream: %w", err)
	}
	return streamFn(w)
}
