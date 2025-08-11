package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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
		err = fmt.Errorf("dst name to long")
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	srcEntry, err := fs.filer.FindEntry(ctx, srcPath)
	if err != nil {
		err = fmt.Errorf("failed to get src entry '%s', err: %s", src, err)
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
	newDir, newName := dstPath.DirAndName()
	newName = util.Nvl(newName, oldName)
	finalDstPath := util.FullPath(newDir).Child(newName)

	dstEntry, err := fs.filer.FindEntry(ctx, util.FullPath(strings.TrimRight(dst, "/")))
	if err != nil && err != filer_pb.ErrNotFound {
		err = fmt.Errorf("failed to get dst entry '%s', err: %s", dst, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	if err == nil && !dstEntry.IsDirectory() && srcEntry.IsDirectory() {
		err = fmt.Errorf("copy: cannot overwrite non-directory '%s' with directory '%s'", dst, src)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	// Copy the file content and chunks
	newEntry, err := fs.copyEntry(ctx, srcEntry, finalDstPath, so)
	if err != nil {
		err = fmt.Errorf("failed to copy entry from '%s' to '%s', err: %s", src, dst, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	if createErr := fs.filer.CreateEntry(ctx, newEntry, false, false, nil, false, fs.filer.MaxFilenameLength); createErr != nil {
		err = fmt.Errorf("failed to create copied entry from '%s' to '%s', err: %s", src, dst, createErr)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	glog.V(1).InfofCtx(ctx, "FilerServer.copy completed successfully: src='%s' -> dst='%s' (final_path='%s')", src, dst, finalDstPath)

	w.WriteHeader(http.StatusNoContent)
}

// copyEntry creates a new entry with copied content and chunks
func (fs *FilerServer) copyEntry(ctx context.Context, srcEntry *filer.Entry, dstPath util.FullPath, so *operation.StorageOption) (*filer.Entry, error) {
	// Create the base entry structure
	newEntry := &filer.Entry{
		FullPath:        dstPath,
		Attr:            srcEntry.Attr,
		Extended:        srcEntry.Extended,
		HardLinkCounter: srcEntry.HardLinkCounter,
		HardLinkId:      srcEntry.HardLinkId,
		Remote:          srcEntry.Remote,
		Quota:           srcEntry.Quota,
	}

	// Handle small files stored in Content field
	if len(srcEntry.Content) > 0 {
		// For small files, just copy the content directly
		newEntry.Content = make([]byte, len(srcEntry.Content))
		copy(newEntry.Content, srcEntry.Content)
		glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied content directly, size=%d", len(newEntry.Content))
		return newEntry, nil
	}

	// Handle files stored as chunks
	if len(srcEntry.GetChunks()) > 0 {
		newChunks, err := fs.copyChunks(ctx, srcEntry.GetChunks(), so)
		if err != nil {
			return nil, fmt.Errorf("failed to copy chunks: %v", err)
		}
		newEntry.Chunks = newChunks
		glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: copied %d chunks", len(newChunks))
		return newEntry, nil
	}

	// Empty file case
	glog.V(2).InfofCtx(ctx, "FilerServer.copyEntry: empty file, no content or chunks to copy")
	return newEntry, nil
}

// copyChunks creates new chunks by copying data from source chunks
func (fs *FilerServer) copyChunks(ctx context.Context, srcChunks []*filer_pb.FileChunk, so *operation.StorageOption) ([]*filer_pb.FileChunk, error) {
	var newChunks []*filer_pb.FileChunk

	for i, srcChunk := range srcChunks {
		glog.V(3).InfofCtx(ctx, "FilerServer.copyChunks: copying chunk %d/%d, size=%d", i+1, len(srcChunks), srcChunk.Size)

		// Read data from source chunk
		chunkData, err := fs.readChunkData(ctx, srcChunk)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk %s: %v", srcChunk.GetFileIdString(), err)
		}

		// Create new chunk with the data
		newChunk, err := fs.writeChunkData(ctx, chunkData, srcChunk.Offset, so)
		if err != nil {
			return nil, fmt.Errorf("failed to write new chunk at offset %d: %v", srcChunk.Offset, err)
		}

		newChunks = append(newChunks, newChunk)
	}

	return newChunks, nil
}

// readChunkData reads the actual data from a chunk
func (fs *FilerServer) readChunkData(ctx context.Context, chunk *filer_pb.FileChunk) ([]byte, error) {
	// Look up volume server for this chunk
	volumeId := chunk.Fid.VolumeId
	lookupResult, err := operation.LookupVolumeIds(fs.filer.GetMaster, fs.grpcDialOption, []string{fmt.Sprintf("%d", volumeId)})
	if err != nil {
		return nil, fmt.Errorf("failed to lookup volume %d: %v", volumeId, err)
	}

	if len(lookupResult) == 0 || len(lookupResult[fmt.Sprintf("%d", volumeId)].Locations) == 0 {
		return nil, fmt.Errorf("no locations found for volume %d", volumeId)
	}

	// Use the first available location
	location := lookupResult[fmt.Sprintf("%d", volumeId)].Locations[0]

	// Read the chunk data
	fileId := chunk.GetFileIdString()
	url := fmt.Sprintf("http://%s/%s", location.Url, fileId)

	glog.V(4).InfofCtx(ctx, "FilerServer.readChunkData: reading from %s", url)

	data, _, err := util_http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk data from %s: %v", url, err)
	}

	// Verify size matches expectation
	if uint64(len(data)) != chunk.Size {
		return nil, fmt.Errorf("chunk size mismatch: expected %d, got %d", chunk.Size, len(data))
	}

	return data, nil
}

// writeChunkData writes data to a new chunk
func (fs *FilerServer) writeChunkData(ctx context.Context, data []byte, offset int64, so *operation.StorageOption) (*filer_pb.FileChunk, error) {
	// Assign a new file ID
	fileId, urlLocation, auth, err := fs.assignNewFileInfo(ctx, so)
	if err != nil {
		return nil, fmt.Errorf("failed to assign new file ID: %v", err)
	}

	glog.V(4).InfofCtx(ctx, "FilerServer.writeChunkData: writing %d bytes to %s", len(data), urlLocation)

	// Create uploader and upload the data
	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, fmt.Errorf("failed to create uploader: %v", err)
	}

	uploadOption := &operation.UploadOption{
		UploadUrl: urlLocation,
		Filename:  "",
		MimeType:  "application/octet-stream",
		Jwt:       auth,
	}

	uploadResult, err, _ := uploader.Upload(ctx, bytes.NewReader(data), uploadOption)
	if err != nil {
		return nil, fmt.Errorf("failed to upload chunk data to %s: %v", urlLocation, err)
	}

	if uploadResult.Error != "" {
		return nil, fmt.Errorf("upload error: %s", uploadResult.Error)
	}

	// Create the chunk metadata using the helper method
	newChunk := uploadResult.ToPbFileChunk(fileId, offset, time.Now().UnixNano())

	return newChunk, nil
}
