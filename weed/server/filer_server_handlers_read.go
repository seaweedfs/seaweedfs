package weed_server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/images"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Validates the preconditions. Returns true if GET/HEAD operation should not proceed.
// Preconditions supported are:
//
//	If-Modified-Since
//	If-Unmodified-Since
//	If-Match
//	If-None-Match
func checkPreconditions(w http.ResponseWriter, r *http.Request, entry *filer.Entry) bool {

	etag := filer.ETagEntry(entry)
	/// When more than one conditional request header field is present in a
	/// request, the order in which the fields are evaluated becomes
	/// important.  In practice, the fields defined in this document are
	/// consistently implemented in a single, logical order, since "lost
	/// update" preconditions have more strict requirements than cache
	/// validation, a validated cache is more efficient than a partial
	/// response, and entity tags are presumed to be more accurate than date
	/// validators. https://tools.ietf.org/html/rfc7232#section-5
	if entry.Attr.Mtime.IsZero() {
		return false
	}
	w.Header().Set("Last-Modified", entry.Attr.Mtime.UTC().Format(http.TimeFormat))

	ifMatchETagHeader := r.Header.Get("If-Match")
	ifUnmodifiedSinceHeader := r.Header.Get("If-Unmodified-Since")
	if ifMatchETagHeader != "" {
		if util.CanonicalizeETag(etag) != util.CanonicalizeETag(ifMatchETagHeader) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	} else if ifUnmodifiedSinceHeader != "" {
		if t, parseError := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader); parseError == nil {
			if t.Before(entry.Attr.Mtime) {
				w.WriteHeader(http.StatusPreconditionFailed)
				return true
			}
		}
	}

	ifNoneMatchETagHeader := r.Header.Get("If-None-Match")
	ifModifiedSinceHeader := r.Header.Get("If-Modified-Since")
	if ifNoneMatchETagHeader != "" {
		if util.CanonicalizeETag(etag) == util.CanonicalizeETag(ifNoneMatchETagHeader) {
			SetEtag(w, etag)
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if ifModifiedSinceHeader != "" {
		if t, parseError := time.Parse(http.TimeFormat, ifModifiedSinceHeader); parseError == nil {
			if !t.Before(entry.Attr.Mtime) {
				SetEtag(w, etag)
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}

	return false
}

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {

	path := r.URL.Path
	isForDirectory := strings.HasSuffix(path, "/")
	if isForDirectory && len(path) > 1 {
		path = path[:len(path)-1]
	}

	entry, err := fs.filer.FindEntry(context.Background(), util.FullPath(path))
	if err != nil {
		if path == "/" {
			fs.listDirectoryHandler(w, r)
			return
		}
		if err == filer_pb.ErrNotFound {
			glog.V(2).Infof("Not found %s: %v", path, err)
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadNotFound).Inc()
			w.WriteHeader(http.StatusNotFound)
		} else {
			glog.Errorf("Internal %s: %v", path, err)
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadInternal).Inc()
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	query := r.URL.Query()

	if entry.IsDirectory() {
		if fs.option.DisableDirListing {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if query.Get("metadata") == "true" {
			writeJsonQuiet(w, r, http.StatusOK, entry)
			return
		}
		if entry.Attr.Mime == "" || (entry.Attr.Mime == s3_constants.FolderMimeType && r.Header.Get(s3_constants.AmzIdentityId) == "") {
			// Don't return directory meta if config value is set to true
			if fs.option.ExposeDirectoryData == false {
				writeJsonError(w, r, http.StatusForbidden, errors.New("directory listing is disabled"))
				return
			}
			// return index of directory for non s3 gateway
			fs.listDirectoryHandler(w, r)
			return
		}
		// inform S3 API this is a user created directory key object
		w.Header().Set(s3_constants.SeaweedFSIsDirectoryKey, "true")
	}

	if isForDirectory && entry.Attr.Mime != s3_constants.FolderMimeType {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if query.Get("metadata") == "true" {
		if query.Get("resolveManifest") == "true" {
			if entry.Chunks, _, err = filer.ResolveChunkManifest(
				fs.filer.MasterClient.GetLookupFileIdFunction(),
				entry.GetChunks(), 0, math.MaxInt64); err != nil {
				err = fmt.Errorf("failed to resolve chunk manifest, err: %s", err.Error())
				writeJsonError(w, r, http.StatusInternalServerError, err)
				return
			}
		}
		writeJsonQuiet(w, r, http.StatusOK, entry)
		return
	}

	if checkPreconditions(w, r, entry) {
		return
	}

	var etag string
	if partNumber, errNum := strconv.Atoi(r.Header.Get(s3_constants.SeaweedFSPartNumber)); errNum == nil {
		if len(entry.Chunks) < partNumber {
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadChunk).Inc()
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("InvalidPart"))
			return
		}
		w.Header().Set(s3_constants.AmzMpPartsCount, strconv.Itoa(len(entry.Chunks)))
		partChunk := entry.GetChunks()[partNumber-1]
		md5, _ := base64.StdEncoding.DecodeString(partChunk.ETag)
		etag = hex.EncodeToString(md5)
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partChunk.Offset, uint64(partChunk.Offset)+partChunk.Size-1))
	} else {
		etag = filer.ETagEntry(entry)
	}
	w.Header().Set("Accept-Ranges", "bytes")

	// mime type
	mimeType := entry.Attr.Mime
	if mimeType == "" {
		if ext := filepath.Ext(entry.Name()); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	// print out the header from extended properties
	for k, v := range entry.Extended {
		if !strings.HasPrefix(k, "xattr-") {
			// "xattr-" prefix is set in filesys.XATTR_PREFIX
			w.Header().Set(k, string(v))
		}
	}

	//Seaweed custom header are not visible to Vue or javascript
	seaweedHeaders := []string{}
	for header := range w.Header() {
		if strings.HasPrefix(header, "Seaweed-") {
			seaweedHeaders = append(seaweedHeaders, header)
		}
	}
	seaweedHeaders = append(seaweedHeaders, "Content-Disposition")
	w.Header().Set("Access-Control-Expose-Headers", strings.Join(seaweedHeaders, ","))

	//set tag count
	tagCount := 0
	for k := range entry.Extended {
		if strings.HasPrefix(k, s3_constants.AmzObjectTagging+"-") {
			tagCount++
		}
	}
	if tagCount > 0 {
		w.Header().Set(s3_constants.AmzTagCount, strconv.Itoa(tagCount))
	}

	SetEtag(w, etag)

	filename := entry.Name()
	AdjustPassthroughHeaders(w, r, filename)
	totalSize := int64(entry.Size())

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	if rangeReq := r.Header.Get("Range"); rangeReq == "" {
		ext := filepath.Ext(filename)
		if len(ext) > 0 {
			ext = strings.ToLower(ext)
		}
		width, height, mode, shouldResize := shouldResizeImages(ext, r)
		if shouldResize {
			data := mem.Allocate(int(totalSize))
			defer mem.Free(data)
			err := filer.ReadAll(data, fs.filer.MasterClient, entry.GetChunks())
			if err != nil {
				glog.Errorf("failed to read %s: %v", path, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			rs, _, _ := images.Resized(ext, bytes.NewReader(data), width, height, mode)
			io.Copy(w, rs)
			return
		}
	}

	ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		if offset+size <= int64(len(entry.Content)) {
			return func(writer io.Writer) error {
				_, err := writer.Write(entry.Content[offset : offset+size])
				if err != nil {
					stats.FilerHandlerCounter.WithLabelValues(stats.ErrorWriteEntry).Inc()
					glog.Errorf("failed to write entry content: %v", err)
				}
				return err
			}, nil
		}
		chunks := entry.GetChunks()
		if entry.IsInRemoteOnly() {
			dir, name := entry.FullPath.DirAndName()
			if resp, err := fs.CacheRemoteObjectToLocalCluster(context.Background(), &filer_pb.CacheRemoteObjectToLocalClusterRequest{
				Directory: dir,
				Name:      name,
			}); err != nil {
				stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadCache).Inc()
				glog.Errorf("CacheRemoteObjectToLocalCluster %s: %v", entry.FullPath, err)
				return nil, fmt.Errorf("cache %s: %v", entry.FullPath, err)
			} else {
				chunks = resp.Entry.GetChunks()
			}
		}

		streamFn, err := filer.PrepareStreamContentWithThrottler(fs.filer.MasterClient, fs.maybeGetVolumeReadJwtAuthorizationToken, chunks, offset, size, fs.option.DownloadMaxBytesPs)
		if err != nil {
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadStream).Inc()
			glog.Errorf("failed to prepare stream content %s: %v", r.URL, err)
			return nil, err
		}
		return func(writer io.Writer) error {
			err := streamFn(writer)
			if err != nil {
				stats.FilerHandlerCounter.WithLabelValues(stats.ErrorReadStream).Inc()
				glog.Errorf("failed to stream content %s: %v", r.URL, err)
			}
			return err
		}, nil
	})
}

func (fs *FilerServer) maybeGetVolumeReadJwtAuthorizationToken(fileId string) string {
	return string(security.GenJwtForVolumeServer(fs.volumeGuard.ReadSigningKey, fs.volumeGuard.ReadExpiresAfterSec, fileId))
}
