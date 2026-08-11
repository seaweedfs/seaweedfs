package weed_server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

const (
	TusVersion              = "1.0.0"
	TusDefaultMaxSize       = int64(5 * 1024 * 1024 * 1024) // 5GB
	TusDefaultSessionExpiry = 24 * time.Hour
	TusUploadsFolder        = ".uploads.tus"
	TusInfoFileName         = ".info"
	TusConsumedFileName     = ".consumed"
	TusChunkExt             = ".chunk"
	TusExtensions           = "creation,creation-with-upload,termination,concatenation"
	TusConcatPartial        = "partial"
	TusConcatFinalPrefix    = "final;"
)

// ErrWormEnforced marks a TUS completion rejected because the target entry is
// WORM-protected. It shares the message the normal write path uses so it maps to
// the same client-facing status.
var ErrWormEnforced = errors.New(constants.ErrMsgOperationNotPermitted)

// TusSession represents an in-progress TUS upload session
type TusSession struct {
	ID         string            `json:"id"`
	TargetPath string            `json:"target_path"`
	Size       int64             `json:"size"`
	Offset     int64             `json:"offset"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	ExpiresAt  time.Time         `json:"expires_at,omitempty"`
	Concat     string            `json:"concat,omitempty"`
	Chunks     []*TusChunkInfo   `json:"chunks,omitempty"`
}

// isPartial reports whether the session is a concatenation partial upload: it
// holds chunks for a later final upload instead of landing at its target path.
func (session *TusSession) isPartial() bool {
	return session.Concat == TusConcatPartial
}

func (session *TusSession) isFinal() bool {
	return strings.HasPrefix(session.Concat, TusConcatFinalPrefix)
}

// TusChunkInfo tracks individual chunk uploads within a session
type TusChunkInfo struct {
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	FileId   string `json:"file_id"`
	UploadAt int64  `json:"upload_at"`
}

// tusSessionDir returns the directory path for storing TUS upload sessions
func (fs *FilerServer) tusSessionDir() string {
	return "/" + TusUploadsFolder
}

// tusSessionPath returns the path to a specific upload session directory
func (fs *FilerServer) tusSessionPath(uploadID string) string {
	return fmt.Sprintf("/%s/%s", TusUploadsFolder, uploadID)
}

// tusSessionInfoPath returns the path to the session info file
func (fs *FilerServer) tusSessionInfoPath(uploadID string) string {
	return fmt.Sprintf("/%s/%s/%s", TusUploadsFolder, uploadID, TusInfoFileName)
}

// tusSessionConsumedPath returns the path of the marker recording that a
// session's chunks belong to a completed upload and must not be freed with it.
func (fs *FilerServer) tusSessionConsumedPath(uploadID string) string {
	return fmt.Sprintf("/%s/%s/%s", TusUploadsFolder, uploadID, TusConsumedFileName)
}

// markTusSessionConsumed claims a session's chunks for a completed upload. With
// exclusive set, a session already claimed by a concurrent request fails with
// filer_pb.ErrEntryAlreadyExists so one partial cannot be consumed twice.
func (fs *FilerServer) markTusSessionConsumed(ctx context.Context, uploadID string, exclusive bool) error {
	return fs.filer.CreateEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(fs.tusSessionConsumedPath(uploadID)),
		Attr: filer.Attr{
			Mode:   0644,
			Crtime: time.Now(),
			Mtime:  time.Now(),
			Uid:    OS_UID,
			Gid:    OS_GID,
		},
	}, nil, exclusive, false, nil, true, fs.filer.MaxFilenameLength)
}

// isTusSessionConsumed fails closed: when the marker cannot be looked up, the
// caller must not treat the session's chunks as free.
// claimTusPartial claims a partial for one final upload, serialized per session
// on this filer, and re-verifies the pinned session under the claim. A failed
// verification releases the claim before returning.
func (fs *FilerServer) claimTusPartial(ctx context.Context, partial *TusSession) error {
	sessionPath := util.FullPath(fs.tusSessionPath(partial.ID))
	pathLock := fs.entryLockTable.AcquireLock("tusClaim", sessionPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(sessionPath, pathLock)

	if err := fs.markTusSessionConsumed(ctx, partial.ID, true); err != nil {
		return err
	}
	if err := fs.refreshTusSessionChunks(ctx, partial); err != nil {
		fs.rollbackTusSessionConsumed(ctx, partial.ID)
		return err
	}
	return nil
}

// rollbackTusSessionConsumed releases a consumed marker after a failed claim or
// completion. A failed rollback wedges the session as consumed: DELETE and
// expiry then preserve its chunks, which leak until removed by fsck.
func (fs *FilerServer) rollbackTusSessionConsumed(ctx context.Context, uploadID string) {
	if err := fs.filer.DeleteEntryMetaAndData(ctx, util.FullPath(fs.tusSessionConsumedPath(uploadID)), false, false, false, false, nil, 0); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.Errorf("TUS session %s wedged as consumed, marker rollback failed: %v", uploadID, err)
	}
}

func (fs *FilerServer) isTusSessionConsumed(ctx context.Context, uploadID string) (bool, error) {
	_, err := fs.filer.FindEntry(ctx, util.FullPath(fs.tusSessionConsumedPath(uploadID)))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, filer_pb.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// tusChunkPath returns the path to store a chunk info file
// Format: /{TusUploadsFolder}/{uploadID}/chunk_{offset}_{size}_{encodedFileId}
func (fs *FilerServer) tusChunkPath(uploadID string, offset, size int64, fileId string) string {
	// Use URL-safe base64 encoding to safely encode fileId (handles both / and _ in fileId)
	encodedFileId := base64.RawURLEncoding.EncodeToString([]byte(fileId))
	return fmt.Sprintf("/%s/%s/chunk_%016d_%016d_%s", TusUploadsFolder, uploadID, offset, size, encodedFileId)
}

// parseTusChunkPath parses chunk info from a chunk entry
// The entry's Crtime is used for the UploadAt timestamp to preserve the actual upload time
func parseTusChunkPath(entry *filer.Entry) (*TusChunkInfo, error) {
	name := entry.Name()
	if !strings.HasPrefix(name, "chunk_") {
		return nil, fmt.Errorf("not a chunk file: %s", name)
	}
	// Use strings.Cut to correctly handle base64-encoded fileId which may contain underscores
	s := name[6:] // Skip "chunk_" prefix
	offsetStr, rest, found := strings.Cut(s, "_")
	if !found {
		return nil, fmt.Errorf("invalid chunk file name format (missing offset): %s", name)
	}
	sizeStr, encodedFileId, found := strings.Cut(rest, "_")
	if !found {
		return nil, fmt.Errorf("invalid chunk file name format (missing size): %s", name)
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid offset in chunk file %q: %w", name, err)
	}
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid size in chunk file %q: %w", name, err)
	}
	// Decode fileId from URL-safe base64
	fileIdBytes, err := base64.RawURLEncoding.DecodeString(encodedFileId)
	if err != nil {
		return nil, fmt.Errorf("invalid fileId encoding in chunk file %q: %w", name, err)
	}
	return &TusChunkInfo{
		Offset:   offset,
		Size:     size,
		FileId:   string(fileIdBytes),
		UploadAt: entry.Crtime.UnixNano(),
	}, nil
}

// createTusSession creates a new TUS upload session
func (fs *FilerServer) createTusSession(ctx context.Context, uploadID, targetPath string, size int64, metadata map[string]string, concat string) (*TusSession, error) {
	session := &TusSession{
		ID:         uploadID,
		TargetPath: targetPath,
		Size:       size,
		Offset:     0,
		Metadata:   metadata,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(fs.option.TusSessionExpiry),
		Concat:     concat,
		Chunks:     []*TusChunkInfo{},
	}

	// Create session directory
	sessionDirPath := util.FullPath(fs.tusSessionPath(uploadID))
	if err := fs.filer.CreateEntry(ctx, &filer.Entry{
		FullPath: sessionDirPath,
		Attr: filer.Attr{
			Mode:   os.ModeDir | 0755,
			Crtime: time.Now(),
			Mtime:  time.Now(),
			Uid:    OS_UID,
			Gid:    OS_GID,
		},
	}, nil, false, false, nil, false, fs.filer.MaxFilenameLength); err != nil {
		return nil, fmt.Errorf("create session directory: %w", err)
	}

	// Save session info
	if err := fs.saveTusSession(ctx, session); err != nil {
		// Cleanup the directory on failure
		fs.filer.DeleteEntryMetaAndData(ctx, sessionDirPath, true, true, false, false, nil, 0)
		return nil, fmt.Errorf("save session info: %w", err)
	}

	glog.V(2).Infof("Created TUS session %s for %s, size=%d", uploadID, targetPath, size)
	return session, nil
}

// saveTusSession saves the session info to the filer
func (fs *FilerServer) saveTusSession(ctx context.Context, session *TusSession) error {
	sessionData, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("marshal session: %w", err)
	}

	infoPath := util.FullPath(fs.tusSessionInfoPath(session.ID))
	entry := &filer.Entry{
		FullPath: infoPath,
		Attr: filer.Attr{
			Mode:   0644,
			Crtime: session.CreatedAt,
			Mtime:  time.Now(),
			Uid:    OS_UID,
			Gid:    OS_GID,
		},
		Content: sessionData,
	}

	if err := fs.filer.CreateEntry(ctx, entry, nil, false, false, nil, false, fs.filer.MaxFilenameLength); err != nil {
		return fmt.Errorf("save session info entry: %w", err)
	}

	return nil
}

// readTusSessionInfo reads and validates a session's immutable .info metadata
// without listing its chunks, the cheap lookup the authorization check needs for
// the stored TargetPath. It rejects a metadata file whose id, target or size is
// unusable so a corrupt or replaced session cannot be authorized or completed.
func (fs *FilerServer) readTusSessionInfo(ctx context.Context, uploadID string) (*TusSession, error) {
	if !isCanonicalTusUploadID(uploadID) {
		return nil, fmt.Errorf("invalid TUS upload id: %q", uploadID)
	}
	infoPath := util.FullPath(fs.tusSessionInfoPath(uploadID))
	entry, err := fs.filer.FindEntry(ctx, infoPath)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil, fmt.Errorf("TUS upload session not found: %s: %w", uploadID, filer_pb.ErrNotFound)
		}
		return nil, fmt.Errorf("find session: %w", err)
	}

	var session TusSession
	if err := json.Unmarshal(entry.Content, &session); err != nil {
		return nil, fmt.Errorf("unmarshal session: %w", err)
	}
	if session.ID != uploadID {
		return nil, fmt.Errorf("TUS session id mismatch: got %q, want %q", session.ID, uploadID)
	}
	target := canonicalTusTargetPath(session.TargetPath)
	if target == "" || target == "/" {
		return nil, fmt.Errorf("invalid TUS target path: %q", session.TargetPath)
	}
	if session.Size < 0 || session.Size > fs.option.TusMaxSize {
		return nil, fmt.Errorf("invalid TUS upload size: %d", session.Size)
	}
	// Pin authorization and every later operation to the same canonical path.
	session.TargetPath = target
	return &session, nil
}

// getTusSession retrieves a validated TUS session by upload ID, including its
// chunks and current offset.
func (fs *FilerServer) getTusSession(ctx context.Context, uploadID string) (*TusSession, error) {
	session, err := fs.readTusSessionInfo(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if err := fs.loadTusSessionChunks(ctx, session); err != nil {
		return nil, err
	}
	return session, nil
}

// loadTusSessionChunks refreshes a session's chunks and offset from its session
// directory, leaving the immutable .info metadata untouched.
func (fs *FilerServer) loadTusSessionChunks(ctx context.Context, session *TusSession) error {
	// Load chunks from directory listing with pagination (atomic read, no race condition)
	sessionDirPath := util.FullPath(fs.tusSessionPath(session.ID))
	session.Chunks = nil
	session.Offset = 0

	lastFileName := ""
	pageSize := 1000
	for {
		entries, hasMore, err := fs.filer.ListDirectoryEntries(ctx, sessionDirPath, lastFileName, false, int64(pageSize), "", "", "")
		if err != nil {
			return fmt.Errorf("list session directory: %w", err)
		}

		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "chunk_") {
				chunk, parseErr := parseTusChunkPath(e)
				if parseErr != nil {
					glog.V(1).Infof("Skipping invalid chunk file %s: %v", e.Name(), parseErr)
					continue
				}
				session.Chunks = append(session.Chunks, chunk)
			}
			lastFileName = e.Name()
		}

		if !hasMore || len(entries) < pageSize {
			break
		}
	}

	// Sort chunks by offset and compute current offset as maximum contiguous range from 0
	if len(session.Chunks) > 0 {
		sort.Slice(session.Chunks, func(i, j int) bool {
			return session.Chunks[i].Offset < session.Chunks[j].Offset
		})
		// Compute the maximum contiguous offset from 0
		// This correctly handles gaps in the upload sequence
		contiguousEnd := int64(0)
		for _, chunk := range session.Chunks {
			if chunk.Offset > contiguousEnd {
				// Gap detected, stop at the first gap
				break
			}
			chunkEnd := chunk.Offset + chunk.Size
			if chunkEnd > contiguousEnd {
				contiguousEnd = chunkEnd
			}
		}
		session.Offset = contiguousEnd
	}

	return nil
}

// verifyTusSessionUnchanged confirms the stored .info still identifies the same
// pinned session.
func (fs *FilerServer) verifyTusSessionUnchanged(ctx context.Context, session *TusSession) error {
	stored, err := fs.readTusSessionInfo(ctx, session.ID)
	if err != nil {
		return err
	}
	if stored.TargetPath != session.TargetPath || stored.Size != session.Size || !stored.CreatedAt.Equal(session.CreatedAt) {
		return fmt.Errorf("TUS session identity changed: %s", session.ID)
	}
	return nil
}

// refreshTusSessionChunks verifies the pinned session still exists and still
// identifies the same upload before refreshing its chunk state, so a PATCH
// cannot complete after a concurrent DELETE or metadata replacement and land at
// a TargetPath other than the one that was authorized.
func (fs *FilerServer) refreshTusSessionChunks(ctx context.Context, session *TusSession) error {
	if err := fs.verifyTusSessionUnchanged(ctx, session); err != nil {
		return err
	}
	return fs.loadTusSessionChunks(ctx, session)
}

// saveTusChunk stores the chunk info as a separate file entry
// This avoids read-modify-write race conditions across multiple filer instances
// The chunk metadata is encoded in the filename; the entry's Crtime preserves upload time
func (fs *FilerServer) saveTusChunk(ctx context.Context, uploadID string, chunk *TusChunkInfo) error {
	if chunk == nil {
		return nil
	}

	// Store chunk info as a separate file entry (atomic operation)
	// Chunk metadata is encoded in the filename; Crtime is used for UploadAt when reading back
	chunkPath := util.FullPath(fs.tusChunkPath(uploadID, chunk.Offset, chunk.Size, chunk.FileId))

	if err := fs.filer.CreateEntry(ctx, &filer.Entry{
		FullPath: chunkPath,
		Attr: filer.Attr{
			Mode:   0644,
			Crtime: time.Now(),
			Mtime:  time.Now(),
			Uid:    OS_UID,
			Gid:    OS_GID,
		},
	}, nil, false, false, nil, false, fs.filer.MaxFilenameLength); err != nil {
		return fmt.Errorf("save chunk info: %w", err)
	}

	return nil
}

// deleteTusSession removes a TUS upload session and all its data
func (fs *FilerServer) deleteTusSession(ctx context.Context, uploadID string) error {
	sessionPath := util.FullPath(fs.tusSessionPath(uploadID))
	pathLock := fs.entryLockTable.AcquireLock("tusDelete", sessionPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(sessionPath, pathLock)

	session, err := fs.getTusSession(ctx, uploadID)
	if err != nil {
		// Session might already be deleted or never existed
		glog.V(1).Infof("TUS session %s not found for deletion: %v", uploadID, err)
		return nil
	}

	// Remove the .info before deciding about chunk data: a final request claims
	// its consumed marker before re-verifying the .info, so once the .info is
	// gone no new claim can pass verification, and a claim that did pass was
	// created earlier and is visible at the check below.
	if err := fs.filer.DeleteEntryMetaAndData(ctx, util.FullPath(fs.tusSessionInfoPath(uploadID)), false, false, false, false, nil, 0); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return fmt.Errorf("delete session info: %w", err)
	}

	// Batch delete all uploaded chunks from volume servers, unless the session
	// was consumed: then the chunks belong to a completed upload's entry.
	consumed, err := fs.isTusSessionConsumed(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("check consumed marker: %w", err)
	}
	if len(session.Chunks) > 0 && !consumed {
		var chunksToDelete []*filer_pb.FileChunk
		for _, chunk := range session.Chunks {
			if chunk.FileId != "" {
				chunksToDelete = append(chunksToDelete, &filer_pb.FileChunk{FileId: chunk.FileId})
			}
		}
		if len(chunksToDelete) > 0 {
			fs.filer.DeleteChunks(ctx, util.FullPath(session.TargetPath), chunksToDelete)
		}
	}

	// Delete the session directory
	sessionDirPath := util.FullPath(fs.tusSessionPath(uploadID))
	if err := fs.filer.DeleteEntryMetaAndData(ctx, sessionDirPath, true, true, false, false, nil, 0); err != nil {
		return fmt.Errorf("delete session directory: %w", err)
	}

	glog.V(2).Infof("Deleted TUS session %s", uploadID)
	return nil
}

// completeTusUpload assembles all chunks and creates the final file
func (fs *FilerServer) completeTusUpload(ctx context.Context, session *TusSession) error {
	if session.Offset != session.Size {
		return fmt.Errorf("upload incomplete: offset=%d, expected=%d", session.Offset, session.Size)
	}

	// Serialize the ownership transition with deleteTusSession on this filer;
	// the marker/.info handshake below covers a delete served by another filer.
	sessionPath := util.FullPath(fs.tusSessionPath(session.ID))
	pathLock := fs.entryLockTable.AcquireLock("tusComplete", sessionPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(sessionPath, pathLock)

	// Sort chunks by offset to ensure correct order
	sort.Slice(session.Chunks, func(i, j int) bool {
		return session.Chunks[i].Offset < session.Chunks[j].Offset
	})

	// Validate chunks are contiguous with no gaps or overlaps
	expectedOffset := int64(0)
	for _, chunk := range session.Chunks {
		if chunk.Offset != expectedOffset {
			return fmt.Errorf("chunk gap or overlap detected: expected offset %d, got %d", expectedOffset, chunk.Offset)
		}
		expectedOffset = chunk.Offset + chunk.Size
	}
	if expectedOffset != session.Size {
		return fmt.Errorf("chunks do not cover full file: chunks end at %d, expected %d", expectedOffset, session.Size)
	}

	// Assemble file chunks in order
	var fileChunks []*filer_pb.FileChunk

	for _, chunk := range session.Chunks {
		fid, fidErr := filer_pb.ToFileIdObject(chunk.FileId)
		if fidErr != nil {
			return fmt.Errorf("invalid file ID %s at offset %d: %w", chunk.FileId, chunk.Offset, fidErr)
		}

		fileChunk := &filer_pb.FileChunk{
			FileId:       chunk.FileId,
			Offset:       chunk.Offset,
			Size:         uint64(chunk.Size),
			ModifiedTsNs: chunk.UploadAt,
			Fid:          fid,
		}
		fileChunks = append(fileChunks, fileChunk)
	}

	// Determine content type from metadata
	contentType := ""
	if session.Metadata != nil {
		if ct, ok := session.Metadata["content-type"]; ok {
			contentType = ct
		}
	}

	// Create the final file entry
	targetPath := util.FullPath(session.TargetPath)

	// Apply the same read-only / WORM protections the normal write path enforces
	// before landing the entry at the client-chosen target path.
	if fs.filer.FilerConf.MatchStorageRule(string(targetPath)).ReadOnly {
		return fmt.Errorf("%w: %s", ErrReadOnly, targetPath)
	}
	if wormEnforced, err := fs.wormEnforcedForEntry(ctx, string(targetPath)); err != nil {
		return fmt.Errorf("check worm: %w", err)
	} else if wormEnforced {
		return ErrWormEnforced
	}

	// Claim the chunks for the entry before creating it: once the marker is
	// durable, a failed cleanup below cannot lead DELETE or expiry to free the
	// entry's chunks. If the entry creation then fails, the client retry re-runs
	// completion; an abandoned marked session leaks its chunks instead of
	// corrupting a live entry.
	if err := fs.markTusSessionConsumed(ctx, session.ID, false); err != nil {
		return fmt.Errorf("mark session consumed: %w", err)
	}

	// Handshake with deleteTusSession, which removes the .info before checking
	// the marker: a session whose .info is still present here cannot have its
	// chunks freed by a delete that missed the marker just made durable.
	if err := fs.verifyTusSessionUnchanged(ctx, session); err != nil {
		fs.rollbackTusSessionConsumed(ctx, session.ID)
		return fmt.Errorf("session deleted before completion: %w", err)
	}

	entry := &filer.Entry{
		FullPath: targetPath,
		Attr: filer.Attr{
			Mode:   0644,
			Crtime: session.CreatedAt,
			Mtime:  time.Now(),
			Uid:    OS_UID,
			Gid:    OS_GID,
			Mime:   contentType,
		},
		Chunks: fileChunks,
	}

	// Ensure parent directory exists
	if err := fs.filer.CreateEntry(ctx, entry, nil, false, false, nil, false, fs.filer.MaxFilenameLength); err != nil {
		return fmt.Errorf("create final file entry: %w", err)
	}

	// Delete the session (but keep the chunks since they're now part of the final file)
	sessionDirPath := util.FullPath(fs.tusSessionPath(session.ID))
	if err := fs.filer.DeleteEntryMetaAndData(ctx, sessionDirPath, true, false, false, false, nil, 0); err != nil {
		glog.V(1).Infof("Failed to cleanup TUS session directory %s: %v", session.ID, err)
	}

	glog.V(2).Infof("Completed TUS upload %s -> %s, size=%d, chunks=%d",
		session.ID, session.TargetPath, session.Size, len(fileChunks))

	return nil
}

// StartTusSessionCleanup starts a background goroutine that periodically cleans up expired TUS sessions
func (fs *FilerServer) StartTusSessionCleanup(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			fs.cleanupExpiredTusSessions()
		}
	}()
	glog.V(0).Infof("TUS session cleanup started with interval %v", interval)
}

// cleanupExpiredTusSessions scans for and removes expired TUS upload sessions
func (fs *FilerServer) cleanupExpiredTusSessions() {
	ctx := context.Background()
	uploadsDir := util.FullPath(fs.tusSessionDir())

	// List all session directories under the TUS uploads folder
	var lastFileName string
	const pageSize = 100

	for {
		entries, hasMore, err := fs.filer.ListDirectoryEntries(ctx, uploadsDir, lastFileName, false, int64(pageSize), "", "", "")
		if err != nil {
			glog.V(1).Infof("TUS cleanup: failed to list sessions: %v", err)
			return
		}

		now := time.Now()
		for _, entry := range entries {
			if !entry.IsDirectory() {
				lastFileName = entry.Name()
				continue
			}

			uploadID := entry.Name()
			session, err := fs.getTusSession(ctx, uploadID)
			if err != nil {
				glog.V(2).Infof("TUS cleanup: skipping session %s: %v", uploadID, err)
				lastFileName = uploadID
				continue
			}

			if !session.ExpiresAt.IsZero() && now.After(session.ExpiresAt) {
				glog.V(1).Infof("TUS cleanup: removing expired session %s (expired at %v)", uploadID, session.ExpiresAt)
				if err := fs.deleteTusSession(ctx, uploadID); err != nil {
					glog.V(1).Infof("TUS cleanup: failed to delete session %s: %v", uploadID, err)
				}
			}

			lastFileName = uploadID
		}

		if !hasMore || len(entries) < pageSize {
			break
		}
	}
}
