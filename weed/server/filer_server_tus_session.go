package weed_server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	TusVersion       = "1.0.0"
	TusMaxSize       = int64(5 * 1024 * 1024 * 1024) // 5GB default max size
	TusUploadsFolder = ".uploads.tus"
	TusInfoFileName  = ".info"
	TusChunkExt      = ".chunk"
	TusExtensions    = "creation,creation-with-upload,termination"
)

// TusSession represents an in-progress TUS upload session
type TusSession struct {
	ID         string            `json:"id"`
	TargetPath string            `json:"target_path"`
	Size       int64             `json:"size"`
	Offset     int64             `json:"offset"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	ExpiresAt  time.Time         `json:"expires_at,omitempty"`
	Chunks     []*TusChunkInfo   `json:"chunks,omitempty"`
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

// createTusSession creates a new TUS upload session
func (fs *FilerServer) createTusSession(ctx context.Context, uploadID, targetPath string, size int64, metadata map[string]string) (*TusSession, error) {
	session := &TusSession{
		ID:         uploadID,
		TargetPath: targetPath,
		Size:       size,
		Offset:     0,
		Metadata:   metadata,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(7 * 24 * time.Hour), // 7 days default expiration
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
	}, false, false, nil, false, fs.filer.MaxFilenameLength); err != nil {
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

	if err := fs.filer.CreateEntry(ctx, entry, false, false, nil, false, fs.filer.MaxFilenameLength); err != nil {
		return fmt.Errorf("save session info entry: %w", err)
	}

	return nil
}

// getTusSession retrieves a TUS session by upload ID
func (fs *FilerServer) getTusSession(ctx context.Context, uploadID string) (*TusSession, error) {
	infoPath := util.FullPath(fs.tusSessionInfoPath(uploadID))
	entry, err := fs.filer.FindEntry(ctx, infoPath)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil, fmt.Errorf("session not found: %s", uploadID)
		}
		return nil, fmt.Errorf("find session: %w", err)
	}

	var session TusSession
	if err := json.Unmarshal(entry.Content, &session); err != nil {
		return nil, fmt.Errorf("unmarshal session: %w", err)
	}

	return &session, nil
}

// updateTusSessionOffset updates the session offset after a successful chunk upload
func (fs *FilerServer) updateTusSessionOffset(ctx context.Context, uploadID string, newOffset int64, chunk *TusChunkInfo) error {
	session, err := fs.getTusSession(ctx, uploadID)
	if err != nil {
		return err
	}

	session.Offset = newOffset
	if chunk != nil {
		session.Chunks = append(session.Chunks, chunk)
	}

	return fs.saveTusSession(ctx, session)
}

// deleteTusSession removes a TUS upload session and all its data
func (fs *FilerServer) deleteTusSession(ctx context.Context, uploadID string) error {
	session, err := fs.getTusSession(ctx, uploadID)
	if err != nil {
		// Session might already be deleted or never existed
		glog.V(1).Infof("TUS session %s not found for deletion: %v", uploadID, err)
		return nil
	}

	// Delete any uploaded chunks from volume servers
	for _, chunk := range session.Chunks {
		if chunk.FileId != "" {
			fs.filer.DeleteChunks(ctx, util.FullPath(session.TargetPath), []*filer_pb.FileChunk{
				{FileId: chunk.FileId},
			})
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

	// Sort chunks by offset to ensure correct order
	sort.Slice(session.Chunks, func(i, j int) bool {
		return session.Chunks[i].Offset < session.Chunks[j].Offset
	})

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
	if err := fs.filer.CreateEntry(ctx, entry, false, false, nil, false, fs.filer.MaxFilenameLength); err != nil {
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
