package weed_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	media_hls "github.com/seaweedfs/seaweedfs/weed/media/hls"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type hlsTsStorageError struct {
	err error
}

func (e *hlsTsStorageError) Error() string { return e.err.Error() }
func (e *hlsTsStorageError) Unwrap() error { return e.err }

func hlsTsIngestErrorStatus(err error) int {
	var storageErr *hlsTsStorageError
	if errors.As(err, &storageErr) {
		return http.StatusInternalServerError
	}
	return http.StatusBadRequest
}

func (fs *FilerServer) hlsTsIngestHandler(w http.ResponseWriter, r *http.Request, sourcePath string) {
	ctx := r.Context()
	if enforced, err := fs.wormEnforcedForEntry(ctx, sourcePath); err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	} else if enforced {
		writeJsonError(w, r, http.StatusForbidden, errors.New("cannot replace WORM-enforced entry"))
		return
	}

	multipartReader, err := r.MultipartReader()
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("HLS ingest requires multipart/form-data: %w", err))
		return
	}
	playlistPart, err := multipartReader.NextPart()
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("read playlist part: %w", err))
		return
	}
	if playlistPart.FormName() != "playlist" {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("first multipart part must be named playlist"))
		return
	}
	playlistBytes, err := io.ReadAll(io.LimitReader(playlistPart, media_hls.MaxPlaylistBytes+1))
	playlistPart.Close()
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("read playlist: %w", err))
		return
	}
	if len(playlistBytes) > media_hls.MaxPlaylistBytes {
		writeJsonError(w, r, http.StatusRequestEntityTooLarge, fmt.Errorf("playlist exceeds %d bytes", media_hls.MaxPlaylistBytes))
		return
	}
	metadata, err := media_hls.ParseSingleFilePlaylist(playlistBytes)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if err := media_hls.Validate(metadata, -1, 0); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	totalSize := media_hls.TotalSize(metadata)
	maxChunkBytes := fs.hlsTsMaxChunkBytes(r)

	mediaPart, err := multipartReader.NextPart()
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("read media part: %w", err))
		return
	}
	if mediaPart.FormName() != "media" {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("second multipart part must be named media"))
		return
	}
	defer mediaPart.Close()

	query := r.URL.Query()
	so, err := fs.detectStorageOption0(ctx, sourcePath, query.Get("collection"), query.Get("replication"), query.Get("ttl"), query.Get("disk"), query.Get("fsync"), query.Get("dataCenter"), query.Get("rack"), query.Get("dataNode"), "false")
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	if so.DiskType == "" {
		so.DiskType = fs.option.DiskType
	}
	if util.FullPath(sourcePath).IsLongerFileName(so.MaxFileNameLength) {
		writeJsonError(w, r, http.StatusRequestURITooLong, filer_pb.ErrEntryNameTooLong)
		return
	}

	var fileChunks []*filer_pb.FileChunk
	cleanup := func() { fs.filer.DeleteUncommittedChunks(context.WithoutCancel(ctx), fileChunks) }
	err = media_hls.WalkSegmentChunks(mediaPart, metadata, maxChunkBytes, func(_ int, chunkOffset int64, data []byte) error {
		if err := media_hls.ValidateTSPackets(data); err != nil {
			return err
		}
		chunks, uploadErr := fs.dataToChunkWithSSE(ctx, r, path.Base(sourcePath), "video/MP2T", data, chunkOffset, so)
		fileChunks = append(fileChunks, chunks...)
		if uploadErr != nil {
			return &hlsTsStorageError{err: uploadErr}
		}
		return nil
	})
	if err != nil {
		cleanup()
		writeJsonError(w, r, hlsTsIngestErrorStatus(err), err)
		return
	}
	if extraPart, nextErr := multipartReader.NextPart(); nextErr != io.EOF {
		cleanup()
		if nextErr == nil {
			extraPart.Close()
			writeJsonError(w, r, http.StatusBadRequest, errors.New("unexpected multipart part after media"))
		} else {
			writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("read multipart end: %w", nextErr))
		}
		return
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	mode := uint64(0660)
	if text := query.Get("mode"); text != "" {
		mode, err = strconv.ParseUint(text, 8, 32)
		if err != nil {
			cleanup()
			writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid mode %q", text))
			return
		}
	}
	now := time.Now()
	entry := &filer.Entry{
		FullPath: util.FullPath(sourcePath),
		Attr:     filer.Attr{Mtime: now, Crtime: now, Mode: os.FileMode(mode), Uid: OS_UID, Gid: OS_GID, TtlSec: so.TtlSeconds, Mime: "video/MP2T", FileSize: uint64(totalSize)},
		Chunks:   fileChunks,
		Extended: map[string][]byte{hlsTsMetadataKey: metadataBytes},
	}
	for _, header := range []string{"Cache-Control", "Expires", "Content-Disposition"} {
		if value := r.Header.Get(header); value != "" {
			entry.Extended[header] = []byte(value)
		}
	}
	if err := fs.filer.CreateEntry(context.WithoutCancel(ctx), entry, nil, false, false, nil, false, so.MaxFileNameLength); err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusCreated, map[string]interface{}{"path": sourcePath, "size": totalSize, "segments": len(metadata.Segments)})
}
