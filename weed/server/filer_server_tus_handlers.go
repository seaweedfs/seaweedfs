package weed_server

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// tusHandler is the main entry point for TUS protocol requests
func (fs *FilerServer) tusHandler(w http.ResponseWriter, r *http.Request) {
	// Set common TUS response headers
	w.Header().Set("Tus-Resumable", TusVersion)

	// OPTIONS is capability discovery only and carries no data, so it is left
	// unauthenticated like the main filer OPTIONS handler. Every other TUS method
	// authenticates the credential first, then authorizes the server-stored
	// TargetPath below, once routing has resolved which resource it acts on.
	var claims *security.SeaweedFilerClaims
	if r.Method != http.MethodOptions {
		tusVersion := r.Header.Get("Tus-Resumable")
		if tusVersion != TusVersion {
			http.Error(w, "Unsupported TUS version", http.StatusPreconditionFailed)
			return
		}

		var authenticated bool
		if claims, authenticated = fs.authenticateFilerJwt(r, r.Method != http.MethodHead); !authenticated {
			writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
			return
		}
	}

	// Route based on method and path
	reqPath := r.URL.Path
	// TusBasePath is pre-normalized in filer_server.go (leading slash, no trailing slash)
	tusPrefix := fs.option.TusBasePath

	// Check if this is an upload location (contains upload ID after {tusPrefix}/.uploads/)
	uploadsPrefix := tusPrefix + "/.uploads/"
	if strings.HasPrefix(reqPath, uploadsPrefix) {
		// Session ids this server mints are canonical UUIDs. Rejecting aliases
		// (a trailing path or any non-canonical spelling) keeps one URL bound to
		// one stored authorization resource.
		uploadID := strings.TrimPrefix(reqPath, uploadsPrefix)
		if !isCanonicalTusUploadID(uploadID) {
			writeTusSessionNotFound(w, r.Method)
			return
		}

		switch r.Method {
		case http.MethodHead, http.MethodPatch, http.MethodDelete:
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		if r.Method == http.MethodPatch {
			ctx = context.WithoutCancel(ctx)
		}
		session, err := fs.readTusSessionInfo(ctx, uploadID)
		if err != nil {
			// A transient filer error resolves to "not found"; log it so it is
			// distinguishable from a genuinely missing session.
			glog.V(1).Infof("TUS session %s not resolved: %v", uploadID, err)
			writeTusSessionNotFound(w, r.Method)
			return
		}
		if !authorizeFilerJwtPaths(r, claims, []string{session.TargetPath}) {
			writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
			return
		}
		// One mutating request per session at a time, like tusd: a PATCH retried
		// while its predecessor is still storing a sub-chunk would otherwise
		// record the same range twice, and a DELETE would race the writer. The
		// chunk state is loaded under this claim so the offset check sees every
		// record the previous request left behind. 423 tells the client to retry.
		if r.Method != http.MethodHead {
			if !fs.lockTusUpload(uploadID) {
				http.Error(w, "Upload is locked by another request", http.StatusLocked)
				return
			}
			defer fs.unlockTusUpload(uploadID)
		}
		if err := fs.loadTusSessionChunks(ctx, session); err != nil {
			glog.Errorf("Failed to load TUS session %s chunks: %v", uploadID, err)
			writeTusSessionNotFound(w, r.Method)
			return
		}

		switch r.Method {
		case http.MethodHead:
			fs.tusHeadHandler(w, session)
		case http.MethodPatch:
			fs.tusPatchHandler(w, r, session)
		case http.MethodDelete:
			fs.tusDeleteHandler(w, r, session)
		}
		return
	}

	// Handle creation endpoints (POST to /.tus/{path})
	switch r.Method {
	case http.MethodOptions:
		fs.tusOptionsHandler(w, r)
	case http.MethodPost:
		if !authorizeFilerJwtPaths(r, claims, []string{fs.tusTargetPath(r)}) {
			writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
			return
		}
		fs.tusCreateHandler(w, r, claims)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// lockTusUpload claims a session for one mutating request; it reports false
// while another request holds the claim.
func (fs *FilerServer) lockTusUpload(uploadID string) bool {
	_, loaded := fs.tusActiveUploads.LoadOrStore(uploadID, struct{}{})
	return !loaded
}

func (fs *FilerServer) unlockTusUpload(uploadID string) {
	fs.tusActiveUploads.Delete(uploadID)
}

// writeTusSessionNotFound answers a request whose session cannot be resolved.
// DELETE is idempotent and returns 204 for a missing session; other verbs 404.
func writeTusSessionNotFound(w http.ResponseWriter, method string) {
	if method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "Upload not found", http.StatusNotFound)
}

// isCanonicalTusUploadID reports whether uploadID is a canonical UUID, the only
// form this server mints, so an aliased or crafted id cannot address a session.
func isCanonicalTusUploadID(uploadID string) bool {
	id, err := uuid.Parse(uploadID)
	return err == nil && id.String() == uploadID
}

// tusTargetPath resolves the filer path a TUS create request targets from the
// request URL. It guarantees a leading slash so the result matches stored
// absolute paths and JWT AllowedPrefixes even if TusBasePath were misconfigured
// with a trailing slash.
func (fs *FilerServer) tusTargetPath(r *http.Request) string {
	target := strings.TrimPrefix(r.URL.Path, fs.option.TusBasePath)
	if target != "" && !strings.HasPrefix(target, "/") {
		target = "/" + target
	}
	return canonicalTusTargetPath(target)
}

// canonicalTusTargetPath normalises an absolute filer target, or returns "" when
// the input is empty or not absolute, so authorization and the final write agree
// on one path.
func canonicalTusTargetPath(target string) string {
	if target == "" || !strings.HasPrefix(target, "/") {
		return ""
	}
	return path.Clean(target)
}

// writeTusCompleteError maps a completeTusUpload failure to the same HTTP status
// the normal write path uses: a read-only prefix returns 507 and a WORM-protected
// target returns 403, rather than a generic 500.
func writeTusCompleteError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrReadOnly):
		http.Error(w, err.Error(), http.StatusInsufficientStorage)
	case errors.Is(err, ErrWormEnforced):
		http.Error(w, err.Error(), http.StatusForbidden)
	default:
		http.Error(w, "Failed to complete upload", http.StatusInternalServerError)
	}
}

// tusOptionsHandler handles OPTIONS requests for capability discovery
func (fs *FilerServer) tusOptionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Tus-Version", TusVersion)
	w.Header().Set("Tus-Extension", TusExtensions)
	w.Header().Set("Tus-Max-Size", strconv.FormatInt(fs.option.TusMaxSize, 10))
	w.WriteHeader(http.StatusOK)
}

// tusCreateHandler handles POST requests to create new uploads
func (fs *FilerServer) tusCreateHandler(w http.ResponseWriter, r *http.Request, claims *security.SeaweedFilerClaims) {
	// Use a context that ignores cancellation from the request context.
	// Internal operations (creating TUS session, writing data, completing uploads)
	// may exceed the filer's client connection inactivity timeout.
	ctx := context.WithoutCancel(r.Context())

	concat := r.Header.Get("Upload-Concat")
	if strings.HasPrefix(concat, TusConcatFinalPrefix) {
		fs.tusConcatFinalHandler(w, r, claims, concat)
		return
	}
	if concat != "" && concat != TusConcatPartial {
		http.Error(w, "Invalid Upload-Concat", http.StatusBadRequest)
		return
	}

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
	if uploadLength > fs.option.TusMaxSize {
		http.Error(w, "Upload-Length exceeds maximum", http.StatusRequestEntityTooLarge)
		return
	}

	// Parse Upload-Metadata header (optional)
	metadata := parseTusMetadata(r.Header.Get("Upload-Metadata"))

	// Determine target path from request URL (leading slash guaranteed)
	targetPath := fs.tusTargetPath(r)
	if targetPath == "" || targetPath == "/" {
		http.Error(w, "Target path required", http.StatusBadRequest)
		return
	}

	// Reject writes to a read-only prefix up front, before creating a session or
	// uploading any chunks, matching the normal write path.
	if fs.filer.FilerConf.MatchStorageRule(targetPath).ReadOnly {
		http.Error(w, ErrReadOnly.Error(), http.StatusInsufficientStorage)
		return
	}

	// Generate upload ID
	uploadID := uuid.New().String()

	// Create upload session
	session, err := fs.createTusSession(ctx, uploadID, targetPath, uploadLength, metadata, concat)
	if err != nil {
		glog.Errorf("Failed to create TUS session: %v", err)
		http.Error(w, "Failed to create upload", http.StatusInternalServerError)
		return
	}

	uploadLocation := fs.tusUploadLocation(uploadID)

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
			bytesWritten, uploadErr := fs.tusWriteData(ctx, r, session, 0, r.Body, r.ContentLength)
			if uploadErr != nil {
				// Cleanup session on failure
				fs.deleteTusSession(ctx, uploadID)
				if errors.Is(uploadErr, ErrContentTooLarge) {
					http.Error(w, "Content-Length exceeds declared upload size", http.StatusRequestEntityTooLarge)
					return
				}
				glog.Errorf("Failed to write initial TUS data: %v", uploadErr)
				http.Error(w, "Failed to write data", http.StatusInternalServerError)
				return
			}

			// Update offset in response header
			w.Header().Set("Upload-Offset", strconv.FormatInt(bytesWritten, 10))

			// Check if upload is complete; a partial upload keeps its chunks for
			// a later concatenation instead of landing at the target path.
			if bytesWritten == session.Size && !session.isPartial() {
				// Ensure the pinned session still exists, then refresh its chunks.
				if err = fs.refreshTusSessionChunks(ctx, session); err != nil {
					glog.Errorf("Failed to get updated TUS session: %v", err)
					http.Error(w, "Failed to complete upload", http.StatusInternalServerError)
					return
				}
				if err := fs.completeTusUpload(ctx, session); err != nil {
					glog.Errorf("Failed to complete TUS upload: %v", err)
					writeTusCompleteError(w, err)
					return
				}
			}
		}
		// ContentLength == 0 is allowed, just proceed to respond
	}

	w.Header().Set("Location", uploadLocation)
	w.WriteHeader(http.StatusCreated)
}

// tusUploadLocation builds the upload URL path for a session id (single leading /)
func (fs *FilerServer) tusUploadLocation(uploadID string) string {
	// TusBasePath is pre-normalized in filer_server.go (leading slash, no trailing slash)
	uploadLocation := path.Clean(fmt.Sprintf("%s/.uploads/%s", fs.option.TusBasePath, uploadID))
	if !strings.HasPrefix(uploadLocation, "/") {
		uploadLocation = "/" + uploadLocation
	}
	return uploadLocation
}

// parseTusConcatFinal extracts the partial upload ids from an Upload-Concat
// final header. Each reference may be an absolute URL or a path, must point at
// this server's upload location, and may appear only once since concatenation
// consumes the partial.
func (fs *FilerServer) parseTusConcatFinal(concat string) ([]string, error) {
	uploadsPrefix := fs.option.TusBasePath + "/.uploads/"
	var partialIDs []string
	seen := make(map[string]bool)
	for _, ref := range strings.Fields(strings.TrimPrefix(concat, TusConcatFinalPrefix)) {
		refURL, err := url.Parse(ref)
		if err != nil {
			return nil, fmt.Errorf("invalid partial upload reference %q", ref)
		}
		partialID := strings.TrimPrefix(refURL.Path, uploadsPrefix)
		if partialID == refURL.Path || !isCanonicalTusUploadID(partialID) {
			return nil, fmt.Errorf("invalid partial upload reference %q", ref)
		}
		if seen[partialID] {
			return nil, fmt.Errorf("duplicate partial upload reference %q", ref)
		}
		seen[partialID] = true
		partialIDs = append(partialIDs, partialID)
	}
	if len(partialIDs) == 0 {
		return nil, errors.New("no partial uploads listed")
	}
	return partialIDs, nil
}

// tusConcatFinalHandler handles POST requests carrying Upload-Concat final. It
// stitches the listed completed partial uploads, in order, into one entry at
// the request's target path.
func (fs *FilerServer) tusConcatFinalHandler(w http.ResponseWriter, r *http.Request, claims *security.SeaweedFilerClaims, concat string) {
	ctx := context.WithoutCancel(r.Context())

	// The final upload's length is the sum of the partial lengths.
	if r.Header.Get("Upload-Length") != "" {
		http.Error(w, "Upload-Length not allowed for a final upload", http.StatusBadRequest)
		return
	}
	if r.ContentLength > 0 {
		http.Error(w, "Cannot upload data to a final upload", http.StatusForbidden)
		return
	}

	partialIDs, err := fs.parseTusConcatFinal(concat)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	targetPath := fs.tusTargetPath(r)
	if targetPath == "" || targetPath == "/" {
		http.Error(w, "Target path required", http.StatusBadRequest)
		return
	}
	if fs.filer.FilerConf.MatchStorageRule(targetPath).ReadOnly {
		http.Error(w, ErrReadOnly.Error(), http.StatusInsufficientStorage)
		return
	}

	// Every partial must exist, be authorized for this credential, and be
	// complete; concatenation-unfinished is not offered. Each one is claimed
	// with an exclusive consumed marker before its chunks are read, so a
	// concurrent final cannot consume the same partial and a concurrent DELETE
	// or expiry cleanup cannot free chunks that move to the final entry.
	var partials []*TusSession
	var claimedIDs []string
	var totalSize int64
	releaseClaims := func() {
		for _, claimedID := range claimedIDs {
			fs.rollbackTusSessionConsumed(ctx, claimedID)
		}
	}
	for _, partialID := range partialIDs {
		partial, err := fs.readTusSessionInfo(ctx, partialID)
		if err != nil {
			glog.V(1).Infof("TUS partial %s not resolved: %v", partialID, err)
			releaseClaims()
			http.Error(w, "Partial upload not found", http.StatusNotFound)
			return
		}
		if !authorizeFilerJwtPaths(r, claims, []string{partial.TargetPath}) {
			releaseClaims()
			writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
			return
		}
		if !partial.isPartial() {
			releaseClaims()
			http.Error(w, "Not a partial upload: "+partialID, http.StatusBadRequest)
			return
		}
		totalSize += partial.Size
		if totalSize > fs.option.TusMaxSize {
			releaseClaims()
			http.Error(w, "Combined upload size exceeds maximum", http.StatusRequestEntityTooLarge)
			return
		}
		if err := fs.claimTusPartial(ctx, partial); err != nil {
			releaseClaims()
			switch {
			case errors.Is(err, filer_pb.ErrEntryAlreadyExists):
				http.Error(w, "Partial upload already consumed: "+partialID, http.StatusConflict)
			case errors.Is(err, filer_pb.ErrNotFound):
				glog.V(1).Infof("TUS partial %s changed before concatenation: %v", partialID, err)
				http.Error(w, "Partial upload not found", http.StatusNotFound)
			default:
				glog.Errorf("Failed to claim TUS partial %s: %v", partialID, err)
				http.Error(w, "Failed to create upload", http.StatusInternalServerError)
			}
			return
		}
		claimedIDs = append(claimedIDs, partialID)
		if partial.Offset != partial.Size {
			releaseClaims()
			http.Error(w, "Partial upload not finished: "+partialID, http.StatusBadRequest)
			return
		}
		partials = append(partials, partial)
	}

	metadata := parseTusMetadata(r.Header.Get("Upload-Metadata"))
	uploadID := uuid.New().String()
	session, err := fs.createTusSession(ctx, uploadID, targetPath, totalSize, metadata, concat)
	if err != nil {
		glog.Errorf("Failed to create TUS session: %v", err)
		releaseClaims()
		http.Error(w, "Failed to create upload", http.StatusInternalServerError)
		return
	}

	// Re-base each partial's chunks onto the final upload's offset space.
	for _, partial := range partials {
		for _, chunk := range partial.Chunks {
			session.Chunks = append(session.Chunks, &TusChunkInfo{
				Offset:   session.Offset + chunk.Offset,
				Size:     chunk.Size,
				FileId:   chunk.FileId,
				UploadAt: chunk.UploadAt,
			})
		}
		session.Offset += partial.Size
	}

	if err := fs.completeTusUpload(ctx, session); err != nil {
		fs.deleteTusSession(ctx, uploadID)
		releaseClaims()
		glog.Errorf("Failed to complete TUS concatenation: %v", err)
		writeTusCompleteError(w, err)
		return
	}

	// The chunks now belong to the final entry, so remove only the partials'
	// session metadata.
	for _, partial := range partials {
		if err := fs.filer.DeleteEntryMetaAndData(ctx, util.FullPath(fs.tusSessionPath(partial.ID)), true, false, false, false, nil, 0); err != nil {
			glog.V(1).Infof("Failed to cleanup TUS partial session %s: %v", partial.ID, err)
		}
	}

	w.Header().Set("Location", fs.tusUploadLocation(uploadID))
	w.WriteHeader(http.StatusCreated)
}

// tusHeadHandler handles HEAD requests to get current upload offset
func (fs *FilerServer) tusHeadHandler(w http.ResponseWriter, session *TusSession) {
	if session.Concat != "" {
		w.Header().Set("Upload-Concat", session.Concat)
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(session.Offset, 10))
	w.Header().Set("Upload-Length", strconv.FormatInt(session.Size, 10))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}

// tusPatchHandler handles PATCH requests to upload data
func (fs *FilerServer) tusPatchHandler(w http.ResponseWriter, r *http.Request, session *TusSession) {
	if session.isFinal() {
		http.Error(w, "Cannot PATCH a final upload", http.StatusForbidden)
		return
	}

	// Use a context that ignores cancellation from the request context.
	// The filer's connection has an inactivity timeout: after the request body is fully read,
	// internal operations (assigning file IDs, uploading to volume servers, completing uploads)
	// may exceed the timeout, causing the request context to be canceled.
	ctx := context.WithoutCancel(r.Context())

	// Validate Content-Type
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/offset+octet-stream" {
		http.Error(w, "Content-Type must be application/offset+octet-stream", http.StatusUnsupportedMediaType)
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
	bytesWritten, err := fs.tusWriteData(ctx, r, session, uploadOffset, r.Body, r.ContentLength)
	if err != nil {
		if errors.Is(err, ErrContentTooLarge) {
			http.Error(w, "Content-Length exceeds remaining upload size", http.StatusRequestEntityTooLarge)
			return
		}
		glog.Errorf("Failed to write TUS data: %v", err)
		http.Error(w, "Failed to write data", http.StatusInternalServerError)
		return
	}

	newOffset := uploadOffset + bytesWritten

	// Check if upload is complete; a partial upload keeps its chunks for a later
	// concatenation instead of landing at the target path.
	if newOffset == session.Size && !session.isPartial() {
		// Ensure the authorized session still exists, then refresh its chunks.
		if err = fs.refreshTusSessionChunks(ctx, session); err != nil {
			glog.Errorf("Failed to get updated TUS session: %v", err)
			http.Error(w, "Failed to complete upload", http.StatusInternalServerError)
			return
		}

		if err := fs.completeTusUpload(ctx, session); err != nil {
			glog.Errorf("Failed to complete TUS upload: %v", err)
			writeTusCompleteError(w, err)
			return
		}
	}

	w.Header().Set("Upload-Offset", strconv.FormatInt(newOffset, 10))
	w.WriteHeader(http.StatusNoContent)
}

// tusDeleteHandler handles DELETE requests to cancel uploads
func (fs *FilerServer) tusDeleteHandler(w http.ResponseWriter, r *http.Request, session *TusSession) {
	ctx := r.Context()

	if err := fs.deleteTusSession(ctx, session.ID); err != nil {
		glog.Errorf("Failed to delete TUS session: %v", err)
		http.Error(w, "Failed to delete upload", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// tusChunkSize is the size of sub-chunks used when streaming uploads to volume servers.
// 4MB balances memory usage (avoiding buffering large TUS chunks) with upload efficiency
// (minimizing the number of volume server requests). Smaller values reduce memory but
// increase request overhead; larger values do the opposite.
const tusChunkSize = 4 * 1024 * 1024 // 4MB

// ErrContentTooLarge is returned when Content-Length exceeds remaining upload space
var ErrContentTooLarge = fmt.Errorf("content length exceeds remaining upload size")

// tusWriteData uploads data to volume servers in streaming chunks and updates session
// It reads data in fixed-size sub-chunks to avoid buffering large TUS chunks entirely in memory
func (fs *FilerServer) tusWriteData(ctx context.Context, r *http.Request, session *TusSession, offset int64, reader io.Reader, contentLength int64) (int64, error) {
	if contentLength == 0 {
		return 0, nil
	}

	// Check if content length exceeds remaining size - return error instead of silently truncating
	remaining := session.Size - offset
	if contentLength > remaining {
		return 0, ErrContentTooLarge
	}
	if remaining <= 0 {
		return 0, nil
	}

	// Determine storage options based on target path
	so, err := fs.detectStorageOption0(ctx, session.TargetPath, "", "", "", "", "", "", "", "", "")
	if err != nil {
		return 0, fmt.Errorf("detect storage option: %w", err)
	}

	// When DiskType is empty, use filer's -disk
	if so.DiskType == "" {
		so.DiskType = fs.option.DiskType
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

		// Store the sub-chunk through the regular write path's chunk writer,
		// which assigns a fresh file id per attempt; a failed attempt's needle
		// is returned so it can be freed instead of lingering unreferenced.
		chunks, chunkErr := fs.dataToChunkWithSSE(ctx, r, "", mimeType, chunkBuf[:n], currentOffset, so)
		if chunkErr != nil {
			fs.filer.DeleteUncommittedChunks(ctx, chunks)
			uploadErr = fmt.Errorf("upload data: %w", chunkErr)
			break
		}
		if len(chunks) == 0 {
			uploadErr = fmt.Errorf("no chunk stored at offset %d", currentOffset)
			break
		}
		stored := chunks[0]

		chunk := &TusChunkInfo{
			Offset:   stored.Offset,
			Size:     int64(stored.Size),
			FileId:   stored.FileId,
			UploadAt: stored.ModifiedTsNs,
		}

		if saveErr := fs.saveTusChunk(ctx, session.ID, chunk); saveErr != nil {
			fs.deleteTusChunk(ctx, session, chunk)
			uploadErr = fmt.Errorf("update session: %w", saveErr)
			break
		}

		totalWritten += chunk.Size
		currentOffset += chunk.Size
		stats.FilerHandlerCounter.WithLabelValues("tusUploadChunk").Inc()
	}

	// Sub-chunks already recorded stay: the session offset a resuming client
	// reads back covers them, and the completed entry is assembled from them.
	return totalWritten, uploadErr
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
