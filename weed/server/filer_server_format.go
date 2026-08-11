package weed_server

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/format"
	_ "github.com/seaweedfs/seaweedfs/weed/format/hlsts"
	_ "github.com/seaweedfs/seaweedfs/weed/format/parquet"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
)

const (
	// Query parameters follow the mv.from/cp.from dotted convention so the
	// general POST endpoint cannot collide with pass-through client params.
	formatIngestParam = "format.ingest"
	formatRepackParam = "format.repack"

	maxFormatSidecarBytes = 16 << 20
	formatSniffBytes      = 512
	// defaultFormatChunkSizeMB caps extent chunks when no maxMB is configured.
	// Extent chunks are buffered in memory, so the limit must never be absent.
	defaultFormatChunkSizeMB = 4
)

// formatChunkSizeLimit mirrors the autoChunk maxMB resolution.
func (fs *FilerServer) formatChunkSizeLimit(r *http.Request) int64 {
	parsedMaxMB, _ := strconv.ParseInt(r.URL.Query().Get("maxMB"), 10, 32)
	maxMB := int32(parsedMaxMB)
	if maxMB <= 0 && fs.option.MaxMB > 0 {
		maxMB = int32(fs.option.MaxMB)
	}
	if maxMB <= 0 {
		maxMB = defaultFormatChunkSizeMB
	}
	return int64(maxMB) * 1024 * 1024
}

// copyStandardHeadersToExtended matches what saveMetaData keeps on an entry.
func copyStandardHeadersToExtended(r *http.Request, extended map[string][]byte) {
	for k, v := range r.Header {
		if len(v) > 0 && len(v[0]) > 0 {
			if strings.HasPrefix(k, needle.PairNamePrefix) || k == "Cache-Control" || k == "Expires" || k == "Content-Disposition" {
				extended[k] = []byte(v[0])
			}
			if k == "Response-Content-Disposition" {
				extended["Content-Disposition"] = []byte(v[0])
			}
		}
	}
}

// formatIngest handles POST /path?format.ingest=<adapter>: a multipart body
// with an "index" sidecar part describing the media's extents, then the
// "media" bytes. Storage chunks are cut on the boundaries the sidecar declares.
func (fs *FilerServer) formatIngest(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) {
	adapterName := r.URL.Query().Get(formatIngestParam)
	adapter := format.ByName(adapterName)
	if adapter == nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("unknown format %q", adapterName))
		return
	}
	sidecarIndexer, ok := adapter.(format.SidecarIndexer)
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("format %q does not support sidecar ingest", adapterName))
		return
	}
	if strings.HasSuffix(r.URL.Path, "/") {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("format ingest target must be a file path"))
		return
	}
	if enforced, err := fs.wormEnforcedForEntry(ctx, r.URL.Path); err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	} else if enforced {
		writeJsonError(w, r, http.StatusForbidden, errors.New("cannot replace WORM-enforced entry"))
		return
	}

	multipartReader, err := r.MultipartReader()
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("format ingest requires multipart/form-data: %w", err))
		return
	}
	sidecarPart, err := multipartReader.NextPart()
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("read index part: %w", err))
		return
	}
	if sidecarPart.FormName() != "index" {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("first multipart part must be named index"))
		return
	}
	sidecar, err := io.ReadAll(io.LimitReader(sidecarPart, maxFormatSidecarBytes+1))
	sidecarPart.Close()
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("read index part: %w", err))
		return
	}
	if len(sidecar) > maxFormatSidecarBytes {
		writeJsonError(w, r, http.StatusRequestEntityTooLarge, fmt.Errorf("index part exceeds %d bytes", maxFormatSidecarBytes))
		return
	}
	layout, err := sidecarIndexer.IndexSidecar(sidecar)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if err := layout.Validate(-1); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

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
	contentType := mediaPart.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	cutter := layout.Cutter(fs.formatChunkSizeLimit(r))
	fileChunks, md5Hash, written, uploadErr, _ := fs.uploadReaderToBoundedChunks(ctx, r, mediaPart, 0, cutter, path.Base(r.URL.Path), contentType, false, so)
	cleanup := func() { fs.filer.DeleteUncommittedChunks(context.WithoutCancel(ctx), fileChunks) }
	if uploadErr != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, uploadErr)
		return
	}
	if total := layout.TotalSize(); written != total {
		cleanup()
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("media is %d bytes but the index describes %d", written, total))
		return
	}
	var extra [1]byte
	if n, _ := io.ReadFull(mediaPart, extra[:]); n != 0 {
		cleanup()
		writeJsonError(w, r, http.StatusBadRequest, errors.New("media has trailing bytes beyond the index"))
		return
	}
	if extraPart, nextErr := multipartReader.NextPart(); nextErr == nil {
		extraPart.Close()
		cleanup()
		writeJsonError(w, r, http.StatusBadRequest, errors.New("unexpected multipart part after media"))
		return
	}

	fileChunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), fileChunks)
	if err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	encoded, err := layout.Encode()
	if err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	mode := uint64(0660)
	if text := r.URL.Query().Get("mode"); text != "" {
		if mode, err = strconv.ParseUint(text, 8, 32); err != nil {
			cleanup()
			writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid mode %q", text))
			return
		}
	}
	now := time.Now()
	entry := &filer.Entry{
		FullPath: util.FullPath(r.URL.Path),
		Attr: filer.Attr{
			Mtime: now, Crtime: now,
			Mode: os.FileMode(mode), Uid: OS_UID, Gid: OS_GID,
			TtlSec: so.TtlSeconds, Mime: contentType,
			Md5: md5Hash.Sum(nil), FileSize: uint64(written),
		},
		Chunks:   fileChunks,
		Extended: map[string][]byte{format.LayoutKey: encoded},
	}
	copyStandardHeadersToExtended(r, entry.Extended)
	// commit under the entry lock like saveMetaData, so ingest overwrites
	// serialize with gRPC writers, renames, and repack
	pathLock := fs.entryLockTable.AcquireLock("formatIngest", entry.FullPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(entry.FullPath, pathLock)
	if err := fs.filer.CreateEntry(context.WithoutCancel(ctx), entry, nil, false, false, nil, skipCheckParentDirEntry(r), so.MaxFileNameLength); err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusCreated, FilerPostResult{Name: entry.Name(), Size: written})
}

// formatRepack handles POST /path?format.repack=<adapter>: it derives the
// layout from the stored bytes and rewrites the entry's chunks cut on extent
// boundaries. The bytes do not change, only where they are cut.
func (fs *FilerServer) formatRepack(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) {
	adapterName := r.URL.Query().Get(formatRepackParam)
	adapter := format.ByName(adapterName)
	if adapter == nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("unknown format %q", adapterName))
		return
	}
	indexer, ok := adapter.(format.Indexer)
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("format %q does not support repack", adapterName))
		return
	}
	fullPath := util.FullPath(r.URL.Path)
	if enforced, err := fs.wormEnforcedForEntry(ctx, r.URL.Path); err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	} else if enforced {
		writeJsonError(w, r, http.StatusForbidden, errors.New("cannot repack WORM-enforced entry"))
		return
	}

	// The lock covers gRPC writers and renames; plain HTTP overwrites do not
	// take it, so repack targets should be quiescent.
	pathLock := fs.entryLockTable.AcquireLock("formatRepack", fullPath, util.ExclusiveLock)
	defer fs.entryLockTable.ReleaseLock(fullPath, pathLock)

	entry, err := fs.filer.FindEntry(ctx, fullPath)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			writeJsonError(w, r, http.StatusNotFound, err)
		} else {
			writeJsonError(w, r, http.StatusInternalServerError, err)
		}
		return
	}
	if entry.IsDirectory() {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("cannot repack a directory"))
		return
	}
	oldChunks := entry.GetChunks()
	if len(oldChunks) == 0 {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("entry has no chunks to repack"))
		return
	}
	if len(entry.HardLinkId) != 0 || entry.Remote != nil {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("cannot repack hard-linked or remote entries"))
		return
	}
	for _, chunk := range oldChunks {
		if chunk.SseType != filer_pb.SSEType_NONE {
			writeJsonError(w, r, http.StatusBadRequest, errors.New("cannot repack server-side encrypted entries"))
			return
		}
	}

	// Repack rewrites where bytes are cut, never their lifetime: new chunks
	// carry the entry's remaining TTL, not the request query's nor a restart
	// of the original span.
	so.TtlSeconds = entry.TtlSec
	if entry.TtlSec > 0 {
		remaining := int64(entry.TtlSec) - int64(time.Since(entry.Crtime)/time.Second)
		if remaining <= 0 {
			writeJsonError(w, r, http.StatusBadRequest, errors.New("entry TTL has already expired"))
			return
		}
		so.TtlSeconds = int32(remaining)
	}

	size := int64(entry.FileSize)
	lookup := fs.filer.MasterClient.GetLookupFileIdFunction()
	chunkViews := filer.ViewFromChunks(ctx, lookup, oldChunks, 0, size)
	readerCache := filer.NewReaderCache(8, chunk_cache.NewChunkCacheInMemory(16), lookup, nil)
	readerAt := filer.NewChunkReaderAtFromClient(ctx, readerCache, chunkViews, size, filer.DefaultPrefetchCount)
	// Close releases the private reader cache and its in-flight prefetches.
	defer readerAt.Close()

	hint := format.Hint{Name: entry.Name(), ContentType: entry.Attr.Mime, Size: size}
	sniffSize := int64(formatSniffBytes)
	if sniffSize > size {
		sniffSize = size
	}
	head := make([]byte, sniffSize)
	if _, err := readerAt.ReadAt(head, 0); err != nil && err != io.EOF {
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("read head: %w", err))
		return
	}
	tail := make([]byte, sniffSize)
	if _, err := readerAt.ReadAt(tail, size-sniffSize); err != nil && err != io.EOF {
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("read tail: %w", err))
		return
	}
	hint.Head, hint.Tail = head, tail
	if !adapter.Sniff(hint) {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("%s does not look like %s", entry.Name(), adapterName))
		return
	}

	layout, err := indexer.Index(ctx, readerAt, size)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if err := layout.Validate(size); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	cutter := layout.Cutter(fs.formatChunkSizeLimit(r))
	newChunks, md5Hash, written, uploadErr, _ := fs.uploadReaderToBoundedChunks(ctx, r, io.NewSectionReader(readerAt, 0, size), 0, cutter, entry.Name(), entry.Attr.Mime, false, so)
	cleanup := func() { fs.filer.DeleteUncommittedChunks(context.WithoutCancel(ctx), newChunks) }
	if uploadErr != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, uploadErr)
		return
	}
	if written != size {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("read %d of %d bytes", written, size))
		return
	}
	newChunks, err = filer.MaybeManifestize(fs.saveAsChunk(ctx, so), newChunks)
	if err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	encoded, err := layout.Encode()
	if err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	newEntry := *entry
	newEntry.Chunks = newChunks
	newEntry.Extended = make(map[string][]byte)
	for k, v := range entry.Extended {
		newEntry.Extended[k] = v
	}
	newEntry.Extended[format.LayoutKey] = encoded
	if len(newEntry.Md5) == 0 {
		newEntry.Md5 = md5Hash.Sum(nil)
	}
	if err := fs.filer.UpdateEntry(context.WithoutCancel(ctx), entry, &newEntry); err != nil {
		cleanup()
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	fs.filer.DeleteChunks(context.WithoutCancel(ctx), fullPath, oldChunks)
	// Filer.UpdateEntry only writes the store; notify subscribers (sync,
	// backup, replication) of the new chunk ids like the gRPC path does.
	fs.filer.NotifyUpdateEvent(ctx, entry, &newEntry, true, false, nil)
	writeJsonQuiet(w, r, http.StatusOK, map[string]interface{}{
		"name": entry.Name(), "size": size, "extents": len(layout.ExtentSizes),
	})
}

// serveFormatView answers GET/HEAD /path?view=<adapter> for entries carrying a
// layout. The layout is advisory: any inconsistency yields 404 here while the
// plain read path stays untouched.
func (fs *FilerServer) serveFormatView(ctx context.Context, w http.ResponseWriter, r *http.Request, entry *filer.Entry, viewName string) {
	adapter := format.ByName(viewName)
	viewer, viewerOk := adapter.(format.Viewer)
	if adapter == nil || !viewerOk {
		http.Error(w, "no such view", http.StatusNotFound)
		return
	}
	encoded := entry.Extended[format.LayoutKey]
	if len(encoded) == 0 {
		http.Error(w, "entry has no format layout", http.StatusNotFound)
		return
	}
	layout, err := format.DecodeLayout(encoded)
	if err != nil || layout.Format != viewName {
		http.Error(w, "entry has no such format layout", http.StatusNotFound)
		return
	}
	if err := layout.Validate(int64(entry.FileSize)); err != nil {
		glog.WarningfCtx(ctx, "stale format layout on %s: %v", entry.FullPath, err)
		http.Error(w, "format layout is stale", http.StatusNotFound)
		return
	}

	// The view's validator must change when the layout or the requested
	// representation changes, even when the media bytes and their MD5 do not:
	// re-ingesting with a different sidecar must invalidate cached views.
	viewIdentity := md5.New()
	viewIdentity.Write([]byte(filer.ETagEntry(entry)))
	viewIdentity.Write(encoded)
	viewIdentity.Write([]byte(r.URL.RawQuery))
	viewEntry := *entry
	viewEntry.Md5 = viewIdentity.Sum(nil)
	if checkPreconditions(w, r, &viewEntry) {
		return
	}

	plan, err := viewer.View(format.ViewRequest{Query: r.URL.Query()}, format.Object{
		Name: entry.Name(), Size: int64(entry.FileSize), Layout: layout,
	})
	if err != nil {
		if errors.Is(err, format.ErrNoSuchView) {
			http.NotFound(w, r)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// pass through stored headers the way plain reads do
	for k, v := range entry.Extended {
		if !strings.HasPrefix(k, "xattr-") && !s3_constants.IsSeaweedFSInternalHeader(k) {
			w.Header().Set(k, string(v))
		}
	}
	// view responses are whole documents or whole extents
	w.Header().Set("Accept-Ranges", "none")
	w.Header().Set("Content-Type", plan.ContentType)
	SetEtag(w, filer.ETagEntry(&viewEntry))

	if plan.Body != nil {
		w.Header().Set("Content-Length", strconv.Itoa(len(plan.Body)))
		if r.Method == http.MethodHead {
			return
		}
		if _, err := w.Write(plan.Body); err != nil {
			glog.V(2).InfofCtx(ctx, "write %s view of %s: %v", viewName, entry.FullPath, err)
		}
		return
	}

	offset, extentSize, ok := layout.ExtentRange(plan.Extent)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(extentSize, 10))
	if r.Method == http.MethodHead {
		return
	}
	if offset+extentSize <= int64(len(entry.Content)) {
		_, _ = w.Write(entry.Content[offset : offset+extentSize])
		return
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	streamFn, err := filer.PrepareStreamContentWithPrefetch(streamCtx, fs.filer.MasterClient, fs.maybeGetVolumeReadJwtAuthorizationToken, entry.GetChunks(), offset, extentSize, fs.option.DownloadMaxBytesPs, filer.DefaultPrefetchCount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := streamFn(w); err != nil {
		glog.ErrorfCtx(ctx, "stream %s view extent %d of %s: %v", viewName, plan.Extent, entry.FullPath, err)
	}
}
