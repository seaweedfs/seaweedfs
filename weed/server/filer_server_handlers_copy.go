package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *FilerServer) copy(ctx context.Context, w http.ResponseWriter, r *http.Request, so *operation.StorageOption) {
	src := r.URL.Query().Get("cp.from")
	dst := r.URL.Path

	glog.V(2).InfofCtx(ctx, "FilerServer.copy %v to %v", src, dst)

	var err error
	if src, err = clearName(src); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if dst, err = clearName(dst); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	src = strings.TrimRight(src, "/")
	if src == "" {
		err = fmt.Errorf("invalid source '/'")
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	srcPath := util.FullPath(src)
	dstPath := util.FullPath(dst)
	if dstPath.IsLongerFileName(so.MaxFileNameLength) {
		err = fmt.Errorf("dst name too long")
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	srcEntry, err := fs.filer.FindEntry(ctx, srcPath)
	if err != nil {
		err = fmt.Errorf("failed to get src entry '%s': %w", src, err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	glog.V(1).InfofCtx(ctx, "FilerServer.copy source entry: content_len=%d, chunks_len=%d", len(srcEntry.Content), len(srcEntry.GetChunks()))

	// Check if source is a directory - currently not supported for recursive copying
	if srcEntry.IsDirectory() {
		err = fmt.Errorf("copy: directory copying not yet supported for '%s'", src)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	_, oldName := srcPath.DirAndName()
	finalDstPath := dstPath

	// Check if destination is a directory
	dstPathEntry, findErr := fs.filer.FindEntry(ctx, dstPath)
	if findErr != nil && findErr != filer_pb.ErrNotFound {
		err = fmt.Errorf("failed to check destination path %s: %w", dstPath, findErr)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	if findErr == nil && dstPathEntry.IsDirectory() {
		finalDstPath = dstPath.Child(oldName)
	} else {
		newDir, newName := dstPath.DirAndName()
		newName = util.Nvl(newName, oldName)
		finalDstPath = util.FullPath(newDir).Child(newName)
	}

	// Check if destination file already exists
	// TODO: add an overwrite parameter to allow overwriting
	if dstEntry, err := fs.filer.FindEntry(ctx, finalDstPath); err != nil && err != filer_pb.ErrNotFound {
		err = fmt.Errorf("failed to check destination entry %s: %w", finalDstPath, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	} else if dstEntry != nil {
		err = fmt.Errorf("destination file %s already exists", finalDstPath)
		writeJsonError(w, r, http.StatusConflict, err)
		return
	}

	// Copy the file content and chunks
	newEntry, err := fs.copyEntry(ctx, srcEntry, finalDstPath, so)
	if err != nil {
		err = fmt.Errorf("failed to copy entry from '%s' to '%s': %w", src, dst, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	if createErr := fs.filer.CreateEntry(ctx, newEntry, true, false, nil, false, fs.filer.MaxFilenameLength); createErr != nil {
		err = fmt.Errorf("failed to create copied entry from '%s' to '%s': %w", src, dst, createErr)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	glog.V(1).InfofCtx(ctx, "FilerServer.copy completed successfully: src='%s' -> dst='%s' (final_path='%s')", src, dst, finalDstPath)

	w.WriteHeader(http.StatusNoContent)
}

// copyEntry creates a new entry with copied content and chunks
func (fs *FilerServer) copyEntry(ctx context.Context, srcEntry *filer.Entry, dstPath util.FullPath, so *operation.StorageOption) (*filer.Entry, error) {
	// Create the base entry structure
	// Note: For hard links, we copy the actual content but NOT the HardLinkId/HardLinkCounter
	// This creates an independent copy rather than another hard link to the same content
	newEntry := &filer.Entry{
		FullPath: dstPath,
		// Deep copy Attr field to ensure slice independence (GroupNames, Md5)
		Attr: func(a filer.Attr) filer.Attr {
			a.GroupNames = append([]string(nil), a.GroupNames...)
			a.Md5 = append([]byte(nil), a.Md5...)
			return a
		}(srcEntry.Attr),
		Quota: srcEntry.Quota,
		// Intentionally NOT copying HardLinkId and HardLinkCounter to create independent copy
	}

	// Deep copy Extended fields to ensure independence
	if srcEntry.Extended != nil {
		newEntry.Extended = make(map[string][]byte, len(srcEntry.Extended))
		for k, v := range srcEntry.Extended {
			newEntry.Extended[k] = append([]byte(nil), v...)
		}
	}

	// Deep copy Remote field to ensure independence
	if srcEntry.Remote != nil {
		newEntry.Remote = &filer_pb.RemoteEntry{
			StorageName:       srcEntry.Remote.StorageName,
			LastLocalSyncTsNs: srcEntry.Remote.LastLocalSyncTsNs,
			RemoteETag:        srcEntry.Remote.RemoteETag,
			RemoteMtime:       srcEntry.Remote.RemoteMtime,
			RemoteSize:        srcEntry.Remote.RemoteSize,
		}
	}

	// Log if we're copying a hard link so we can track this behavior
	if len(srcEntry.HardLinkId) > 0 {
		glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copying hard link %s (nlink=%d) as independent file", srcEntry.FullPath, srcEntry.HardLinkCounter)
	}

	// Handle small files stored in Content field
	if len(srcEntry.Content) > 0 {
		// For small files, just copy the content directly
		newEntry.Content = make([]byte, len(srcEntry.Content))
		copy(newEntry.Content, srcEntry.Content)
		glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied content directly, size=%d", len(newEntry.Content))
		return newEntry, nil
	}

	// Handle files stored as chunks (including resolved hard link content)
	if len(srcEntry.GetChunks()) > 0 {
		srcChunks := srcEntry.GetChunks()

		// Create HTTP client once for reuse across all chunk operations
		client := &http.Client{Timeout: 60 * time.Second}

		// Check if any chunks are manifest chunks - these require special handling
		if filer.HasChunkManifest(srcChunks) {
			glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: handling manifest chunks")
			newChunks, err := fs.copyChunksWithManifest(ctx, srcChunks, so, client)
			if err != nil {
				return nil, fmt.Errorf("failed to copy chunks with manifest: %w", err)
			}
			newEntry.Chunks = newChunks
			glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied manifest chunks, count=%d", len(newChunks))
		} else {
			// Regular chunks without manifest - copy directly
			newChunks, err := fs.copyChunks(ctx, srcChunks, so, client)
			if err != nil {
				return nil, fmt.Errorf("failed to copy chunks: %w", err)
			}
			newEntry.Chunks = newChunks
			glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied regular chunks, count=%d", len(newChunks))
		}
		return newEntry, nil
	}

	// Empty file case (or hard link with no content - should not happen if hard link was properly resolved)
	if len(srcEntry.HardLinkId) > 0 {
		glog.WarningfCtx(ctx, "FilerServer.copyEntry: hard link %s appears to have no content - this may indicate an issue with hard link resolution", srcEntry.FullPath)
	}
	glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: empty file, no content or chunks to copy")
	return newEntry, nil
}

// copyChunks creates new chunks by copying data from source chunks using parallel streaming approach
func (fs *FilerServer) copyChunks(ctx context.Context, srcChunks []*filer_pb.FileChunk, so *operation.StorageOption, client *http.Client) ([]*filer_pb.FileChunk, error) {
	if len(srcChunks) == 0 {
		return nil, nil
	}

	// Optimize: Batch volume lookup for all chunks to reduce RPC calls
	volumeLocationsMap, err := fs.batchLookupVolumeLocations(ctx, srcChunks)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup volume locations: %w", err)
	}

	// Parallel chunk copying with concurrency control using errgroup
	const maxConcurrentChunks = 8 // Match SeaweedFS standard for parallel operations

	// Pre-allocate result slice to maintain order
	newChunks := make([]*filer_pb.FileChunk, len(srcChunks))

	// Use errgroup for cleaner concurrency management
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentChunks) // Limit concurrent goroutines

	// Validate that all chunk locations are available before starting any concurrent work
	for _, chunk := range srcChunks {
		volumeId := chunk.Fid.VolumeId
		locations, ok := volumeLocationsMap[volumeId]
		if !ok || len(locations) == 0 {
			return nil, fmt.Errorf("no locations found for volume %d", volumeId)
		}
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunks: starting parallel copy of %d chunks with max concurrency %d", len(srcChunks), maxConcurrentChunks)

	// Launch goroutines for each chunk
	for i, srcChunk := range srcChunks {
		// Capture loop variables for goroutine closure
		chunkIndex := i
		chunk := srcChunk
		chunkLocations := volumeLocationsMap[srcChunk.Fid.VolumeId]

		g.Go(func() error {
			glog.V(3).InfofCtx(gCtx, "FilerServer.copyChunks: copying chunk %d/%d, size=%d", chunkIndex+1, len(srcChunks), chunk.Size)

			// Use streaming copy to avoid loading entire chunk into memory
			newChunk, err := fs.streamCopyChunk(gCtx, chunk, so, client, chunkLocations)
			if err != nil {
				return fmt.Errorf("failed to copy chunk %d (%s): %w", chunkIndex+1, chunk.GetFileIdString(), err)
			}

			// Store result at correct index to maintain order
			newChunks[chunkIndex] = newChunk

			glog.V(4).InfofCtx(gCtx, "FilerServer.copyChunks: successfully copied chunk %d/%d", chunkIndex+1, len(srcChunks))
			return nil
		})
	}

	// Wait for all chunks to complete and return first error (if any)
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Verify all chunks were copied (shouldn't happen if no errors, but safety check)
	for i, chunk := range newChunks {
		if chunk == nil {
			return nil, fmt.Errorf("chunk %d was not copied (internal error)", i)
		}
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunks: successfully completed parallel copy of %d chunks", len(srcChunks))
	return newChunks, nil
}

// copyChunksWithManifest handles copying chunks that include manifest chunks
func (fs *FilerServer) copyChunksWithManifest(ctx context.Context, srcChunks []*filer_pb.FileChunk, so *operation.StorageOption, client *http.Client) ([]*filer_pb.FileChunk, error) {
	if len(srcChunks) == 0 {
		return nil, nil
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: processing %d chunks (some are manifests)", len(srcChunks))

	// Separate manifest chunks from regular data chunks
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(srcChunks)

	var newChunks []*filer_pb.FileChunk

	// First, copy all non-manifest chunks directly
	if len(nonManifestChunks) > 0 {
		glog.V(3).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: copying %d non-manifest chunks", len(nonManifestChunks))
		newNonManifestChunks, err := fs.copyChunks(ctx, nonManifestChunks, so, client)
		if err != nil {
			return nil, fmt.Errorf("failed to copy non-manifest chunks: %w", err)
		}
		newChunks = append(newChunks, newNonManifestChunks...)
	}

	// Process each manifest chunk separately
	for i, manifestChunk := range manifestChunks {
		glog.V(3).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: processing manifest chunk %d/%d", i+1, len(manifestChunks))

		// Resolve the manifest chunk to get the actual data chunks it references
		lookupFileIdFn := func(ctx context.Context, fileId string) (urls []string, err error) {
			return fs.filer.MasterClient.GetLookupFileIdFunction()(ctx, fileId)
		}

		resolvedChunks, err := filer.ResolveOneChunkManifest(ctx, lookupFileIdFn, manifestChunk)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve manifest chunk %s: %w", manifestChunk.GetFileIdString(), err)
		}

		glog.V(4).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: resolved manifest chunk %s to %d data chunks",
			manifestChunk.GetFileIdString(), len(resolvedChunks))

		// Copy all the resolved data chunks (use recursive copyChunksWithManifest to handle nested manifests)
		newResolvedChunks, err := fs.copyChunksWithManifest(ctx, resolvedChunks, so, client)
		if err != nil {
			return nil, fmt.Errorf("failed to copy resolved chunks from manifest %s: %w", manifestChunk.GetFileIdString(), err)
		}

		// Create a new manifest chunk that references the copied data chunks
		newManifestChunk, err := fs.createManifestChunk(ctx, newResolvedChunks, manifestChunk, so, client)
		if err != nil {
			return nil, fmt.Errorf("failed to create new manifest chunk: %w", err)
		}

		newChunks = append(newChunks, newManifestChunk)

		glog.V(4).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: created new manifest chunk %s for %d resolved chunks",
			newManifestChunk.GetFileIdString(), len(newResolvedChunks))
	}

	glog.V(2).InfofCtx(ctx, "FilerServer.copyChunksWithManifest: completed copying %d total chunks (%d manifest, %d regular)",
		len(newChunks), len(manifestChunks), len(nonManifestChunks))

	return newChunks, nil
}

// createManifestChunk creates a new manifest chunk that references the provided data chunks
func (fs *FilerServer) createManifestChunk(ctx context.Context, dataChunks []*filer_pb.FileChunk, originalManifest *filer_pb.FileChunk, so *operation.StorageOption, client *http.Client) (*filer_pb.FileChunk, error) {
	// Create the manifest data structure
	filer_pb.BeforeEntrySerialization(dataChunks)

	manifestData := &filer_pb.FileChunkManifest{
		Chunks: dataChunks,
	}

	// Serialize the manifest
	data, err := proto.Marshal(manifestData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Save the manifest data as a new chunk
	saveFunc := func(reader io.Reader, name string, offset int64, tsNs int64) (chunk *filer_pb.FileChunk, err error) {
		// Assign a new file ID
		fileId, urlLocation, auth, assignErr := fs.assignNewFileInfo(ctx, so)
		if assignErr != nil {
			return nil, fmt.Errorf("failed to assign file ID for manifest: %w", assignErr)
		}

		// Upload the manifest data
		err = fs.uploadData(ctx, reader, urlLocation, string(auth), client)
		if err != nil {
			return nil, fmt.Errorf("failed to upload manifest data: %w", err)
		}

		// Create the chunk metadata
		chunk = &filer_pb.FileChunk{
			FileId: fileId,
			Offset: offset,
			Size:   uint64(len(data)),
		}
		return chunk, nil
	}

	manifestChunk, err := saveFunc(bytes.NewReader(data), "", originalManifest.Offset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to save manifest chunk: %w", err)
	}

	// Set manifest-specific properties
	manifestChunk.IsChunkManifest = true
	manifestChunk.Size = originalManifest.Size

	return manifestChunk, nil
}

// uploadData uploads data to a volume server
func (fs *FilerServer) uploadData(ctx context.Context, reader io.Reader, urlLocation, auth string, client *http.Client) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", urlLocation, reader)
	if err != nil {
		return fmt.Errorf("failed to create upload request: %w", err)
	}

	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("upload failed with status %d, and failed to read response: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// batchLookupVolumeLocations performs a single batched lookup for all unique volume IDs in the chunks
func (fs *FilerServer) batchLookupVolumeLocations(ctx context.Context, chunks []*filer_pb.FileChunk) (map[uint32][]operation.Location, error) {
	// Collect unique volume IDs and their string representations to avoid repeated conversions
	volumeIdMap := make(map[uint32]string)
	for _, chunk := range chunks {
		vid := chunk.Fid.VolumeId
		if _, found := volumeIdMap[vid]; !found {
			volumeIdMap[vid] = fmt.Sprintf("%d", vid)
		}
	}

	if len(volumeIdMap) == 0 {
		return make(map[uint32][]operation.Location), nil
	}

	// Convert to slice of strings for the lookup call
	volumeIdStrs := make([]string, 0, len(volumeIdMap))
	for _, vidStr := range volumeIdMap {
		volumeIdStrs = append(volumeIdStrs, vidStr)
	}

	// Perform single batched lookup
	lookupResult, err := operation.LookupVolumeIds(fs.filer.GetMaster, fs.grpcDialOption, volumeIdStrs)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup volumes: %w", err)
	}

	// Convert result to map of volumeId -> locations
	volumeLocationsMap := make(map[uint32][]operation.Location)
	for volumeId, volumeIdStr := range volumeIdMap {
		if volumeLocations, ok := lookupResult[volumeIdStr]; ok && len(volumeLocations.Locations) > 0 {
			volumeLocationsMap[volumeId] = volumeLocations.Locations
		}
	}

	return volumeLocationsMap, nil
}

// streamCopyChunk copies a chunk using streaming to minimize memory usage
func (fs *FilerServer) streamCopyChunk(ctx context.Context, srcChunk *filer_pb.FileChunk, so *operation.StorageOption, client *http.Client, locations []operation.Location) (*filer_pb.FileChunk, error) {
	// Assign a new file ID for destination
	fileId, urlLocation, auth, err := fs.assignNewFileInfo(ctx, so)
	if err != nil {
		return nil, fmt.Errorf("failed to assign new file ID: %w", err)
	}

	// Try all available locations for source chunk until one succeeds
	fileIdString := srcChunk.GetFileIdString()
	var lastErr error

	for i, location := range locations {
		srcUrl := fmt.Sprintf("http://%s/%s", location.Url, fileIdString)
		glog.V(4).InfofCtx(ctx, "FilerServer.streamCopyChunk: attempting streaming copy from %s to %s (attempt %d/%d)", srcUrl, urlLocation, i+1, len(locations))

		// Perform streaming copy using HTTP client
		err := fs.performStreamCopy(ctx, srcUrl, urlLocation, string(auth), srcChunk.Size, client)
		if err != nil {
			lastErr = err
			glog.V(2).InfofCtx(ctx, "FilerServer.streamCopyChunk: failed streaming copy from %s: %v", srcUrl, err)
			continue
		}

		// Success - create chunk metadata
		newChunk := &filer_pb.FileChunk{
			FileId: fileId,
			Offset: srcChunk.Offset,
			Size:   srcChunk.Size,
			ETag:   srcChunk.ETag,
		}

		glog.V(4).InfofCtx(ctx, "FilerServer.streamCopyChunk: successfully streamed %d bytes", srcChunk.Size)
		return newChunk, nil
	}

	// All locations failed
	return nil, fmt.Errorf("failed to stream copy chunk from any location: %w", lastErr)
}

// performStreamCopy performs the actual streaming copy from source URL to destination URL
func (fs *FilerServer) performStreamCopy(ctx context.Context, srcUrl, dstUrl, auth string, expectedSize uint64, client *http.Client) error {
	// Create HTTP request to read from source
	req, err := http.NewRequestWithContext(ctx, "GET", srcUrl, nil)
	if err != nil {
		return fmt.Errorf("failed to create source request: %v", err)
	}

	// Perform source request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to read from source: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("source returned status %d", resp.StatusCode)
	}

	// Create HTTP request to write to destination
	dstReq, err := http.NewRequestWithContext(ctx, "PUT", dstUrl, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to create destination request: %v", err)
	}
	dstReq.ContentLength = int64(expectedSize)

	// Set authorization header if provided
	if auth != "" {
		dstReq.Header.Set("Authorization", "Bearer "+auth)
	}
	dstReq.Header.Set("Content-Type", "application/octet-stream")

	// Perform destination request
	dstResp, err := client.Do(dstReq)
	if err != nil {
		return fmt.Errorf("failed to write to destination: %v", err)
	}
	defer dstResp.Body.Close()

	if dstResp.StatusCode != http.StatusCreated && dstResp.StatusCode != http.StatusOK {
		// Read error response body for more details
		body, readErr := io.ReadAll(dstResp.Body)
		if readErr != nil {
			return fmt.Errorf("destination returned status %d, and failed to read body: %w", dstResp.StatusCode, readErr)
		}
		return fmt.Errorf("destination returned status %d: %s", dstResp.StatusCode, string(body))
	}

	glog.V(4).InfofCtx(ctx, "FilerServer.performStreamCopy: successfully streamed data from %s to %s", srcUrl, dstUrl)
	return nil
}
