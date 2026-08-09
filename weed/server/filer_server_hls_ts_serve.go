package weed_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	media_hls "github.com/seaweedfs/seaweedfs/weed/media/hls"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const maxHlsTsFileSize = uint64(1<<63 - 1)

func (fs *FilerServer) loadHlsTsEntry(ctx context.Context, sourcePath string) (*filer.Entry, *media_hls.Metadata, error) {
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(sourcePath))
	if err != nil {
		return nil, nil, err
	}
	metadataBytes := entry.Extended[hlsTsMetadataKey]
	if len(metadataBytes) == 0 {
		return nil, nil, filer_pb.ErrNotFound
	}
	metadata := &media_hls.Metadata{}
	if err := json.Unmarshal(metadataBytes, metadata); err != nil {
		return nil, nil, fmt.Errorf("decode HLS TS metadata: %w", err)
	}
	if entry.FileSize > maxHlsTsFileSize {
		return nil, nil, fmt.Errorf("HLS TS file size %d exceeds int64 range", entry.FileSize)
	}
	if err := media_hls.Validate(metadata, int64(entry.FileSize), 0); err != nil {
		return nil, nil, fmt.Errorf("validate HLS TS metadata: %w", err)
	}
	return entry, metadata, nil
}

func applyHlsTsPassthroughHeaders(w http.ResponseWriter, entry *filer.Entry) {
	for _, header := range []string{"Cache-Control", "Expires"} {
		if value := entry.Extended[header]; len(value) != 0 {
			w.Header().Set(header, string(value))
		}
	}
}

func (fs *FilerServer) hlsTsPlaylistHandler(w http.ResponseWriter, r *http.Request, sourcePath string) {
	entry, metadata, err := fs.loadHlsTsEntry(r.Context(), sourcePath)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	body := media_hls.RenderMediaPlaylist(metadata)
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	applyHlsTsPassthroughHeaders(w, entry)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(body)
}

func hlsTsSegmentForSequence(metadata *media_hls.Metadata, sequence int64) (media_hls.Segment, bool) {
	if metadata == nil || sequence < metadata.MediaSequence {
		return media_hls.Segment{}, false
	}
	index := sequence - metadata.MediaSequence
	if index >= int64(len(metadata.Segments)) {
		return media_hls.Segment{}, false
	}
	return metadata.Segments[index], true
}

func (fs *FilerServer) hlsTsSegmentHandler(w http.ResponseWriter, r *http.Request, sourcePath string, sequence int64) {
	entry, metadata, err := fs.loadHlsTsEntry(r.Context(), sourcePath)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	segment, ok := hlsTsSegmentForSequence(metadata, sequence)
	if !ok {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "video/MP2T")
	w.Header().Set("Content-Length", strconv.FormatInt(segment.Size, 10))
	applyHlsTsPassthroughHeaders(w, entry)
	if r.Method == http.MethodHead {
		return
	}

	streamCtx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()
	// A segment may span several chunks when it is larger than the chunk limit.
	// Prefetch overlaps the next chunk's volume fetch with delivering the
	// current one, and stays within the requested [offset,size) range so it
	// never reads past the segment. A single-chunk segment has nothing to
	// prefetch, so this matches the plain streaming path there.
	streamFn, err := filer.PrepareStreamContentWithPrefetch(streamCtx, fs.filer.MasterClient, fs.maybeGetVolumeReadJwtAuthorizationToken, entry.GetChunks(), segment.Offset, segment.Size, fs.option.DownloadMaxBytesPs, hlsTsSegmentPrefetch)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := streamFn(w); err != nil {
		glog.ErrorfCtx(r.Context(), "HLS TS segment %s sequence %d: %v", sourcePath, sequence, err)
	}
}
