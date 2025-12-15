package weed_server

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// tusHandler is the main entry point for TUS protocol requests
func (fs *FilerServer) tusHandler(w http.ResponseWriter, r *http.Request) {
	// Set common TUS response headers
	w.Header().Set("Tus-Resumable", TusVersion)

	// Check Tus-Resumable header for non-OPTIONS requests
	if r.Method != http.MethodOptions {
		tusVersion := r.Header.Get("Tus-Resumable")
		if tusVersion != TusVersion {
			http.Error(w, "Unsupported TUS version", http.StatusPreconditionFailed)
			return
		}
	}

	// Route based on method and path
	reqPath := r.URL.Path
	// TusPath is pre-normalized in filer_server.go (leading slash, no trailing slash)
	tusPrefix := fs.option.TusPath

	// Check if this is an upload location (contains upload ID after {tusPrefix}/.uploads/)
	uploadsPrefix := tusPrefix + "/.uploads/"
	if strings.HasPrefix(reqPath, uploadsPrefix) {
		uploadID := strings.TrimPrefix(reqPath, uploadsPrefix)
		uploadID = strings.Split(uploadID, "/")[0] // Get just the ID, not any trailing path

		switch r.Method {
		case http.MethodHead:
			fs.tusHeadHandler(w, r, uploadID)
		case http.MethodPatch:
			fs.tusPatchHandler(w, r, uploadID)
		case http.MethodDelete:
			fs.tusDeleteHandler(w, r, uploadID)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}

	// Handle creation endpoints (POST to /.tus/{path})
	switch r.Method {
	case http.MethodOptions:
		fs.tusOptionsHandler(w, r)
	case http.MethodPost:
		fs.tusCreateHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// tusOptionsHandler handles OPTIONS requests for capability discovery
func (fs *FilerServer) tusOptionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Tus-Version", TusVersion)
	w.Header().Set("Tus-Extension", TusExtensions)
	w.Header().Set("Tus-Max-Size", strconv.FormatInt(TusMaxSize, 10))
	w.WriteHeader(http.StatusOK)
}

// tusCreateHandler handles POST requests to create new uploads
func (fs *FilerServer) tusCreateHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parse Upload-Length header (required)
	uploadLengthStr := r.Header.Get("Upload-Length")
	if uploadLengthStr == "" {
		http.Error(w, "Upload-Length header required", http.StatusBadRequest)
		return
	}
	uploadLength, err := strconv.ParseInt(uploadLengthStr, 10, 64)
	if err != nil || uploadLength < 0 {
		http.Error(w, "Invalid Upload-Length", http.StatusBadRequest)
		return
	}
	if uploadLength > TusMaxSize {
		http.Error(w, "Upload-Length exceeds maximum", http.StatusRequestEntityTooLarge)
		return
	}

	// Parse Upload-Metadata header (optional)
	metadata := parseTusMetadata(r.Header.Get("Upload-Metadata"))

	// TusPath is pre-normalized in filer_server.go (leading slash, no trailing slash)
	tusPrefix := fs.option.TusPath

	// Determine target path from request URL
	targetPath := strings.TrimPrefix(r.URL.Path, tusPrefix)
	if targetPath == "" || targetPath == "/" {
		http.Error(w, "Target path required", http.StatusBadRequest)
		return
	}

	// Generate upload ID
	uploadID := uuid.New().String()

	// Create upload session
	session, err := fs.createTusSession(ctx, uploadID, targetPath, uploadLength, metadata)
	if err != nil {
		glog.Errorf("Failed to create TUS session: %v", err)
		http.Error(w, "Failed to create upload", http.StatusInternalServerError)
		return
	}

	// Build upload location URL (ensure it starts with single /)
	uploadLocation := path.Clean(fmt.Sprintf("%s/.uploads/%s", tusPrefix, uploadID))
	if !strings.HasPrefix(uploadLocation, "/") {
		uploadLocation = "/" + uploadLocation
	}

	// Handle creation-with-upload extension
	// TUS requires Content-Length for uploads; reject chunked encoding
	if r.Header.Get("Content-Type") == "application/offset+octet-stream" {
		if r.ContentLength < 0 {
			fs.deleteTusSession(ctx, uploadID)
			http.Error(w, "Content-Length header required for creation-with-upload", http.StatusBadRequest)
			return
		}
		if r.ContentLength > 0 {
			// Upload data in the creation request
			bytesWritten, uploadErr := fs.tusWriteData(ctx, session, 0, r.Body, r.ContentLength)
			if uploadErr != nil {
				// Cleanup session on failure
				fs.deleteTusSession(ctx, uploadID)
				glog.Errorf("Failed to write initial TUS data: %v", uploadErr)
				http.Error(w, "Failed to write data", http.StatusInternalServerError)
				return
			}

			// Update offset in response header
			w.Header().Set("Upload-Offset", strconv.FormatInt(bytesWritten, 10))

			// Check if upload is complete
			if bytesWritten == session.Size {
				// Refresh session to get updated chunks
				session, err = fs.getTusSession(ctx, uploadID)
				if err != nil {
					glog.Errorf("Failed to get updated TUS session: %v", err)
					http.Error(w, "Failed to complete upload", http.StatusInternalServerError)
					return
				}
				if err := fs.completeTusUpload(ctx, session); err != nil {
					glog.Errorf("Failed to complete TUS upload: %v", err)
					http.Error(w, "Failed to complete upload", http.StatusInternalServerError)
					return
				}
			}
		}
		// ContentLength == 0 is allowed, just proceed to respond
	}

	w.Header().Set("Location", uploadLocation)
	w.WriteHeader(http.StatusCreated)
}

// tusHeadHandler handles HEAD requests to get current upload offset
func (fs *FilerServer) tusHeadHandler(w http.ResponseWriter, r *http.Request, uploadID string) {
	ctx := r.Context()

	session, err := fs.getTusSession(ctx, uploadID)
	if err != nil {
		http.Error(w, "Upload not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Upload-Offset", strconv.FormatInt(session.Offset, 10))
	w.Header().Set("Upload-Length", strconv.FormatInt(session.Size, 10))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}

// tusPatchHandler handles PATCH requests to upload data
func (fs *FilerServer) tusPatchHandler(w http.ResponseWriter, r *http.Request, uploadID string) {
	ctx := r.Context()

	// Validate Content-Type
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/offset+octet-stream" {
		http.Error(w, "Content-Type must be application/offset+octet-stream", http.StatusUnsupportedMediaType)
		return
	}

	// Get current session
	session, err := fs.getTusSession(ctx, uploadID)
	if err != nil {
		http.Error(w, "Upload not found", http.StatusNotFound)
		return
	}

	// Validate Upload-Offset header
	uploadOffsetStr := r.Header.Get("Upload-Offset")
	if uploadOffsetStr == "" {
		http.Error(w, "Upload-Offset header required", http.StatusBadRequest)
		return
	}
	uploadOffset, err := strconv.ParseInt(uploadOffsetStr, 10, 64)
	if err != nil || uploadOffset < 0 {
		http.Error(w, "Invalid Upload-Offset", http.StatusBadRequest)
		return
	}

	// Check offset matches current position
	if uploadOffset != session.Offset {
		http.Error(w, fmt.Sprintf("Offset mismatch: expected %d, got %d", session.Offset, uploadOffset), http.StatusConflict)
		return
	}

	// TUS requires Content-Length header for PATCH requests
	if r.ContentLength < 0 {
		http.Error(w, "Content-Length header required", http.StatusBadRequest)
		return
	}

	// Write data
	bytesWritten, err := fs.tusWriteData(ctx, session, uploadOffset, r.Body, r.ContentLength)
	if err != nil {
		glog.Errorf("Failed to write TUS data: %v", err)
		http.Error(w, "Failed to write data", http.StatusInternalServerError)
		return
	}

	newOffset := uploadOffset + bytesWritten

	// Check if upload is complete
	if newOffset == session.Size {
		// Refresh session to get updated chunks
		session, err = fs.getTusSession(ctx, uploadID)
		if err != nil {
			glog.Errorf("Failed to get updated TUS session: %v", err)
			http.Error(w, "Failed to complete upload", http.StatusInternalServerError)
			return
		}

		if err := fs.completeTusUpload(ctx, session); err != nil {
			glog.Errorf("Failed to complete TUS upload: %v", err)
			http.Error(w, "Failed to complete upload", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Upload-Offset", strconv.FormatInt(newOffset, 10))
	w.WriteHeader(http.StatusNoContent)
}

// tusDeleteHandler handles DELETE requests to cancel uploads
func (fs *FilerServer) tusDeleteHandler(w http.ResponseWriter, r *http.Request, uploadID string) {
	ctx := r.Context()

	if err := fs.deleteTusSession(ctx, uploadID); err != nil {
		glog.Errorf("Failed to delete TUS session: %v", err)
		http.Error(w, "Failed to delete upload", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// tusChunkSize is the maximum size of each sub-chunk when uploading TUS data
// This prevents buffering the entire TUS chunk in memory
const tusChunkSize = 4 * 1024 * 1024 // 4MB

// tusWriteData uploads data to volume servers in streaming chunks and updates session
// It reads data in fixed-size sub-chunks to avoid buffering large TUS chunks entirely in memory
func (fs *FilerServer) tusWriteData(ctx context.Context, session *TusSession, offset int64, reader io.Reader, contentLength int64) (int64, error) {
	if contentLength == 0 {
		return 0, nil
	}

	// Limit content length to remaining size
	remaining := session.Size - offset
	if contentLength > remaining {
		contentLength = remaining
	}
	if contentLength <= 0 {
		return 0, nil
	}

	// Determine storage options based on target path
	so, err := fs.detectStorageOption0(ctx, session.TargetPath, "", "", "", "", "", "", "", "", "")
	if err != nil {
		return 0, fmt.Errorf("detect storage option: %w", err)
	}

	// Read first bytes for MIME type detection
	sniffSize := int64(512)
	if contentLength < sniffSize {
		sniffSize = contentLength
	}
	sniffBuf := make([]byte, sniffSize)
	sniffN, sniffErr := io.ReadFull(reader, sniffBuf)
	if sniffErr != nil && sniffErr != io.EOF && sniffErr != io.ErrUnexpectedEOF {
		return 0, fmt.Errorf("read data for mime detection: %w", sniffErr)
	}
	if sniffN == 0 {
		return 0, nil
	}
	sniffBuf = sniffBuf[:sniffN]
	mimeType := http.DetectContentType(sniffBuf)

	// Create a combined reader with sniffed bytes prepended
	var dataReader io.Reader
	if int64(sniffN) >= contentLength {
		dataReader = bytes.NewReader(sniffBuf)
	} else {
		dataReader = io.MultiReader(bytes.NewReader(sniffBuf), io.LimitReader(reader, contentLength-int64(sniffN)))
	}

	// Upload in streaming chunks to avoid buffering entire content in memory
	var totalWritten int64
	var uploadErr error
	var uploadedChunks []*TusChunkInfo

	// Create one uploader for all sub-chunks to reuse HTTP client connections
	uploader, uploaderErr := operation.NewUploader()
	if uploaderErr != nil {
		return 0, fmt.Errorf("create uploader: %w", uploaderErr)
	}

	chunkBuf := make([]byte, tusChunkSize)
	currentOffset := offset

	for totalWritten < contentLength {
		// Read up to tusChunkSize bytes
		readSize := int64(tusChunkSize)
		if contentLength-totalWritten < readSize {
			readSize = contentLength - totalWritten
		}

		n, readErr := io.ReadFull(dataReader, chunkBuf[:readSize])
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			uploadErr = fmt.Errorf("read chunk data: %w", readErr)
			break
		}
		if n == 0 {
			break
		}

		chunkData := chunkBuf[:n]

		// Assign file ID from master for this sub-chunk
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(ctx, so)
		if assignErr != nil {
			uploadErr = fmt.Errorf("assign volume: %w", assignErr)
			break
		}

		// Upload to volume server using BytesReader (avoids double buffering in uploader)
		uploadResult, uploadResultErr, _ := uploader.Upload(ctx, util.NewBytesReader(chunkData), &operation.UploadOption{
			UploadUrl:         urlLocation,
			Filename:          "",
			Cipher:            fs.option.Cipher,
			IsInputCompressed: false,
			MimeType:          mimeType,
			PairMap:           nil,
			Jwt:               auth,
		})
		if uploadResultErr != nil {
			uploadErr = fmt.Errorf("upload data: %w", uploadResultErr)
			break
		}

		// Create chunk info and save it
		chunk := &TusChunkInfo{
			Offset:   currentOffset,
			Size:     int64(uploadResult.Size),
			FileId:   fileId,
			UploadAt: time.Now().UnixNano(),
		}

		if saveErr := fs.saveTusChunk(ctx, session.ID, chunk); saveErr != nil {
			// Cleanup this chunk on failure
			fs.filer.DeleteChunks(ctx, util.FullPath(session.TargetPath), []*filer_pb.FileChunk{
				{FileId: fileId},
			})
			uploadErr = fmt.Errorf("update session: %w", saveErr)
			break
		}

		uploadedChunks = append(uploadedChunks, chunk)

		totalWritten += int64(uploadResult.Size)
		currentOffset += int64(uploadResult.Size)
		stats.FilerHandlerCounter.WithLabelValues("tusUploadChunk").Inc()
	}

	if uploadErr != nil {
		// Cleanup all uploaded chunks on error
		if len(uploadedChunks) > 0 {
			var chunksToDelete []*filer_pb.FileChunk
			for _, c := range uploadedChunks {
				chunksToDelete = append(chunksToDelete, &filer_pb.FileChunk{FileId: c.FileId})
			}
			fs.filer.DeleteChunks(ctx, util.FullPath(session.TargetPath), chunksToDelete)
		}
		return 0, uploadErr
	}

	return totalWritten, nil
}

// parseTusMetadata parses the Upload-Metadata header
// Format: key1 base64value1,key2 base64value2,...
func parseTusMetadata(header string) map[string]string {
	metadata := make(map[string]string)
	if header == "" {
		return metadata
	}

	pairs := strings.Split(header, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		parts := strings.SplitN(pair, " ", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		encodedValue := strings.TrimSpace(parts[1])

		value, err := base64.StdEncoding.DecodeString(encodedValue)
		if err != nil {
			glog.V(1).Infof("Failed to decode TUS metadata value for key %s: %v", key, err)
			continue
		}
		metadata[key] = string(value)
	}

	return metadata
}
