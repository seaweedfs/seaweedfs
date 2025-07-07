package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"modernc.org/strutil"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

const (
	DirectiveCopy    = "COPY"
	DirectiveReplace = "REPLACE"
)

func (s3a *S3ApiServer) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {

	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)

	glog.V(3).Infof("CopyObjectHandler %s %s => %s %s", srcBucket, srcObject, dstBucket, dstObject)

	replaceMeta, replaceTagging := replaceDirective(r.Header)

	if (srcBucket == dstBucket && srcObject == dstObject || cpSrcPath == "") && (replaceMeta || replaceTagging) {
		fullPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject))
		dir, name := fullPath.DirAndName()
		entry, err := s3a.getEntry(dir, name)
		if err != nil || entry.IsDirectory {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
			return
		}
		entry.Extended, err = processMetadataBytes(r.Header, entry.Extended, replaceMeta, replaceTagging)
		entry.Attributes.Mtime = time.Now().Unix()
		if err != nil {
			glog.Errorf("CopyObjectHandler ValidateTags error %s: %v", r.URL, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidTag)
			return
		}
		err = s3a.touch(dir, name, entry)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
			return
		}
		writeSuccessResponseXML(w, r, CopyObjectResult{
			ETag:         fmt.Sprintf("%x", entry.Attributes.Md5),
			LastModified: time.Now().UTC(),
		})
		return
	}

	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}
	srcPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, srcBucket, srcObject))
	dir, name := srcPath.DirAndName()
	entry, err := s3a.getEntry(dir, name)
	if err != nil || entry.IsDirectory {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	if srcBucket == dstBucket && srcObject == dstObject {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopyDest)
		return
	}

	// Create new entry for destination
	dstEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: entry.Attributes.FileSize,
			Mtime:    time.Now().Unix(),
			Crtime:   entry.Attributes.Crtime,
			Mime:     entry.Attributes.Mime,
		},
		Extended: make(map[string][]byte),
	}

	// Copy extended attributes from source
	for k, v := range entry.Extended {
		dstEntry.Extended[k] = v
	}

	// Process metadata and tags and apply to destination
	processedMetadata, tagErr := processMetadataBytes(r.Header, entry.Extended, replaceMeta, replaceTagging)
	if tagErr != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	// Apply processed metadata to destination entry
	for k, v := range processedMetadata {
		dstEntry.Extended[k] = v
	}

	// For zero-size files or files without chunks, use the original approach
	if entry.Attributes.FileSize == 0 || len(entry.GetChunks()) == 0 {
		// Just copy the entry structure without chunks for zero-size files
		dstEntry.Chunks = nil
	} else {
		// Replicate chunks for files with content
		dstChunks, err := s3a.copyChunks(entry, r.URL.Path)
		if err != nil {
			glog.Errorf("CopyObjectHandler copy chunks error: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
		dstEntry.Chunks = dstChunks
	}

	// Save the new entry
	dstPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject))
	dstDir, dstName := dstPath.DirAndName()
	if err := s3a.mkFile(dstDir, dstName, dstEntry.Chunks, func(entry *filer_pb.Entry) {
		entry.Attributes = dstEntry.Attributes
		entry.Extended = dstEntry.Extended
	}); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Convert filer_pb.Entry to filer.Entry for ETag calculation
	filerEntry := &filer.Entry{
		FullPath: dstPath,
		Attr: filer.Attr{
			FileSize: dstEntry.Attributes.FileSize,
			Mtime:    time.Unix(dstEntry.Attributes.Mtime, 0),
			Crtime:   time.Unix(dstEntry.Attributes.Crtime, 0),
			Mime:     dstEntry.Attributes.Mime,
		},
		Chunks: dstEntry.Chunks,
	}

	setEtag(w, filer.ETagEntry(filerEntry))

	response := CopyObjectResult{
		ETag:         filer.ETagEntry(filerEntry),
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, r, response)

}

func pathToBucketAndObject(path string) (bucket, object string) {
	path = strings.TrimPrefix(path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 2 {
		return parts[0], "/" + parts[1]
	}
	return parts[0], "/"
}

type CopyPartResult struct {
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
}

func (s3a *S3ApiServer) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingRESTMPUapi.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	partIDString := r.URL.Query().Get("partNumber")
	uploadID := r.URL.Query().Get("uploadId")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}

	// Check if the upload ID is valid
	err = s3a.checkUploadId(dstObject, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	glog.V(3).Infof("CopyObjectPartHandler %s %s => %s part %d upload %s", srcBucket, srcObject, dstBucket, partID, uploadID)

	// check partID with maximum part ID for multipart objects
	if partID > globalMaxPartID {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxParts)
		return
	}

	// Get source entry
	srcPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, srcBucket, srcObject))
	dir, name := srcPath.DirAndName()
	entry, err := s3a.getEntry(dir, name)
	if err != nil || entry.IsDirectory {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	// Handle range header if present
	rangeHeader := r.Header.Get("x-amz-copy-source-range")
	var startOffset, endOffset int64
	if rangeHeader != "" {
		startOffset, endOffset, err = parseRangeHeader(rangeHeader)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRange)
			return
		}
	} else {
		startOffset = 0
		if entry.Attributes.FileSize == 0 {
			endOffset = -1 // For zero-size files, use -1 as endOffset
		} else {
			endOffset = int64(entry.Attributes.FileSize) - 1
		}
	}

	// Create new entry for the part
	dstEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileSize: uint64(endOffset - startOffset + 1),
			Mtime:    time.Now().Unix(),
			Crtime:   time.Now().Unix(),
			Mime:     entry.Attributes.Mime,
		},
		Extended: make(map[string][]byte),
	}

	// Handle zero-size files or empty ranges
	if entry.Attributes.FileSize == 0 || endOffset < startOffset {
		// For zero-size files or invalid ranges, create an empty part
		dstEntry.Chunks = nil
	} else {
		// Copy chunks that overlap with the range
		dstChunks, err := s3a.copyChunksForRange(entry, startOffset, endOffset, r.URL.Path)
		if err != nil {
			glog.Errorf("CopyObjectPartHandler copy chunks error: %v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
		dstEntry.Chunks = dstChunks
	}

	// Save the part entry to the multipart uploads folder
	uploadDir := s3a.genUploadsFolder(dstBucket) + "/" + uploadID
	partName := fmt.Sprintf("%04d_%s.part", partID, "copy")

	if err := s3a.mkFile(uploadDir, partName, dstEntry.Chunks, func(entry *filer_pb.Entry) {
		entry.Attributes = dstEntry.Attributes
		entry.Extended = dstEntry.Extended
	}); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Calculate ETag for the part
	partPath := util.FullPath(uploadDir + "/" + partName)
	filerEntry := &filer.Entry{
		FullPath: partPath,
		Attr: filer.Attr{
			FileSize: dstEntry.Attributes.FileSize,
			Mtime:    time.Unix(dstEntry.Attributes.Mtime, 0),
			Crtime:   time.Unix(dstEntry.Attributes.Crtime, 0),
			Mime:     dstEntry.Attributes.Mime,
		},
		Chunks: dstEntry.Chunks,
	}

	etag := filer.ETagEntry(filerEntry)
	setEtag(w, etag)

	response := CopyPartResult{
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, r, response)
}

func replaceDirective(reqHeader http.Header) (replaceMeta, replaceTagging bool) {
	return reqHeader.Get(s3_constants.AmzUserMetaDirective) == DirectiveReplace, reqHeader.Get(s3_constants.AmzObjectTaggingDirective) == DirectiveReplace
}

func processMetadata(reqHeader, existing http.Header, replaceMeta, replaceTagging bool, getTags func(parentDirectoryPath string, entryName string) (tags map[string]string, err error), dir, name string) (err error) {
	if sc := reqHeader.Get(s3_constants.AmzStorageClass); len(sc) == 0 {
		if sc := existing[s3_constants.AmzStorageClass]; len(sc) > 0 {
			reqHeader[s3_constants.AmzStorageClass] = sc
		}
	}

	if !replaceMeta {
		for header, _ := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
				delete(reqHeader, header)
			}
		}
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
				reqHeader[k] = v
			}
		}
	}

	if !replaceTagging {
		for header, _ := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzObjectTagging) {
				delete(reqHeader, header)
			}
		}

		found := false
		for k, _ := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTaggingPrefix) {
				found = true
				break
			}
		}

		if found {
			tags, err := getTags(dir, name)
			if err != nil {
				return err
			}

			var tagArr []string
			for k, v := range tags {
				tagArr = append(tagArr, fmt.Sprintf("%s=%s", k, v))
			}
			tagStr := strutil.JoinFields(tagArr, "&")
			reqHeader.Set(s3_constants.AmzObjectTagging, tagStr)
		}
	}
	return
}

func processMetadataBytes(reqHeader http.Header, existing map[string][]byte, replaceMeta, replaceTagging bool) (metadata map[string][]byte, err error) {
	metadata = make(map[string][]byte)

	if sc := existing[s3_constants.AmzStorageClass]; len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = sc
	}
	if sc := reqHeader.Get(s3_constants.AmzStorageClass); len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	if replaceMeta {
		for header, values := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
				for _, value := range values {
					metadata[header] = []byte(value)
				}
			}
		}
	} else {
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
				metadata[k] = v
			}
		}
	}
	if replaceTagging {
		if tags := reqHeader.Get(s3_constants.AmzObjectTagging); tags != "" {
			parsedTags, err := parseTagsHeader(tags)
			if err != nil {
				return nil, err
			}
			err = ValidateTags(parsedTags)
			if err != nil {
				return nil, err
			}
			for k, v := range parsedTags {
				metadata[s3_constants.AmzObjectTagging+"-"+k] = []byte(v)
			}
		}
	} else {
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTagging) {
				metadata[k] = v
			}
		}
		delete(metadata, s3_constants.AmzTagCount)
	}

	return
}

// copyChunks replicates chunks from source entry to destination entry
func (s3a *S3ApiServer) copyChunks(entry *filer_pb.Entry, dstPath string) ([]*filer_pb.FileChunk, error) {
	dstChunks := make([]*filer_pb.FileChunk, len(entry.GetChunks()))
	executor := util.NewLimitedConcurrentExecutor(4) // Limit to 4 concurrent operations
	errChan := make(chan error, len(entry.GetChunks()))

	for i, chunk := range entry.GetChunks() {
		chunkIndex := i
		executor.Execute(func() {
			dstChunk, err := s3a.copySingleChunk(chunk, dstPath)
			if err != nil {
				errChan <- fmt.Errorf("chunk %d: %v", chunkIndex, err)
				return
			}
			dstChunks[chunkIndex] = dstChunk
			errChan <- nil
		})
	}

	// Wait for all operations to complete and check for errors
	for i := 0; i < len(entry.GetChunks()); i++ {
		if err := <-errChan; err != nil {
			return nil, err
		}
	}

	return dstChunks, nil
}

// copySingleChunk copies a single chunk from source to destination
func (s3a *S3ApiServer) copySingleChunk(chunk *filer_pb.FileChunk, dstPath string) (*filer_pb.FileChunk, error) {
	// Create a new chunk with same properties but new file ID
	dstChunk := &filer_pb.FileChunk{
		Offset:       chunk.Offset,
		Size:         chunk.Size,
		ModifiedTsNs: time.Now().UnixNano(),
		ETag:         chunk.ETag,
		IsCompressed: chunk.IsCompressed,
		CipherKey:    chunk.CipherKey,
	}

	// Get new file ID using filer's AssignVolume
	assignResult, err := s3a.assignNewVolume(dstPath)
	if err != nil {
		return nil, fmt.Errorf("assign volume: %v", err)
	}

	dstChunk.FileId = assignResult.FileId
	fid, err := filer_pb.ToFileIdObject(assignResult.FileId)
	if err != nil {
		return nil, fmt.Errorf("parse file ID: %v", err)
	}
	dstChunk.Fid = fid

	// Get source URL using LookupFileId
	srcUrl, _, err := operation.LookupFileId(func(_ context.Context) pb.ServerAddress {
		return pb.ServerAddress(s3a.option.Filer.ToGrpcAddress())
	}, s3a.option.GrpcDialOption, chunk.GetFileIdString())
	if err != nil {
		return nil, fmt.Errorf("lookup source file ID: %v", err)
	}

	// Download and upload the chunk
	if err := s3a.transferChunkData(srcUrl, assignResult, chunk.Offset, int64(chunk.Size)); err != nil {
		return nil, fmt.Errorf("transfer chunk data: %v", err)
	}

	return dstChunk, nil
}

// copySingleChunkForRange copies a portion of a chunk for range operations
func (s3a *S3ApiServer) copySingleChunkForRange(originalChunk, rangeChunk *filer_pb.FileChunk, rangeStart, rangeEnd int64, dstPath string) (*filer_pb.FileChunk, error) {
	// Create a new chunk with same properties but new file ID
	dstChunk := &filer_pb.FileChunk{
		Offset:       rangeChunk.Offset,
		Size:         rangeChunk.Size,
		ModifiedTsNs: time.Now().UnixNano(),
		ETag:         rangeChunk.ETag,
		IsCompressed: rangeChunk.IsCompressed,
		CipherKey:    rangeChunk.CipherKey,
	}

	// Get new file ID using filer's AssignVolume
	assignResult, err := s3a.assignNewVolume(dstPath)
	if err != nil {
		return nil, fmt.Errorf("assign volume: %v", err)
	}

	dstChunk.FileId = assignResult.FileId
	fid, err := filer_pb.ToFileIdObject(assignResult.FileId)
	if err != nil {
		return nil, fmt.Errorf("parse file ID: %v", err)
	}
	dstChunk.Fid = fid

	// Get source URL using LookupFileId
	srcUrl, _, err := operation.LookupFileId(func(_ context.Context) pb.ServerAddress {
		return pb.ServerAddress(s3a.option.Filer.ToGrpcAddress())
	}, s3a.option.GrpcDialOption, originalChunk.GetFileIdString())
	if err != nil {
		return nil, fmt.Errorf("lookup source file ID: %v", err)
	}

	// Calculate the portion of the original chunk that we need to copy
	chunkStart := originalChunk.Offset
	overlapStart := max(rangeStart, chunkStart)
	offsetInChunk := overlapStart - chunkStart

	// Download and upload the chunk portion
	if err := s3a.transferChunkRangeData(srcUrl, assignResult, offsetInChunk, int64(rangeChunk.Size)); err != nil {
		return nil, fmt.Errorf("transfer chunk range data: %v", err)
	}

	return dstChunk, nil
}

// assignNewVolume assigns a new volume for the chunk
func (s3a *S3ApiServer) assignNewVolume(dstPath string) (*filer_pb.AssignVolumeResponse, error) {
	var assignResult *filer_pb.AssignVolumeResponse
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: "",
			Collection:  "",
			DiskType:    "",
			DataCenter:  s3a.option.DataCenter,
			Path:        dstPath,
		})
		if err != nil {
			return fmt.Errorf("assign volume: %v", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("assign volume: %v", resp.Error)
		}
		assignResult = resp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return assignResult, nil
}

// transferChunkData downloads a portion of the chunk from source and uploads it to destination
func (s3a *S3ApiServer) transferChunkData(srcUrl string, assignResult *filer_pb.AssignVolumeResponse, offset, size int64) error {
	dstUrl := fmt.Sprintf("http://%s/%s", assignResult.Location.Url, assignResult.FileId)

	// Read the chunk data using ReadUrlAsStream
	var chunkData []byte
	shouldRetry, err := util_http.ReadUrlAsStream(context.Background(), srcUrl, nil, false, false, 0, int(size), func(data []byte) {
		chunkData = append(chunkData, data...)
	})
	if err != nil {
		return fmt.Errorf("download chunk: %v", err)
	}
	if shouldRetry {
		return fmt.Errorf("download chunk: retry needed")
	}

	// Upload chunk to new location
	uploadOption := &operation.UploadOption{
		UploadUrl:         dstUrl,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               security.EncodedJwt(assignResult.Auth),
	}
	uploader, err := operation.NewUploader()
	if err != nil {
		return fmt.Errorf("create uploader: %v", err)
	}
	_, err = uploader.UploadData(context.Background(), chunkData, uploadOption)
	if err != nil {
		return fmt.Errorf("upload chunk: %v", err)
	}

	return nil
}

// transferChunkRangeData downloads a specific range of data from the source chunk and uploads it to destination
func (s3a *S3ApiServer) transferChunkRangeData(srcUrl string, assignResult *filer_pb.AssignVolumeResponse, offsetInChunk, size int64) error {
	dstUrl := fmt.Sprintf("http://%s/%s", assignResult.Location.Url, assignResult.FileId)

	// Read the specific range of chunk data using ReadUrlAsStream
	var chunkData []byte
	shouldRetry, err := util_http.ReadUrlAsStream(context.Background(), srcUrl, nil, false, false, offsetInChunk, int(size), func(data []byte) {
		chunkData = append(chunkData, data...)
	})
	if err != nil {
		return fmt.Errorf("download chunk range: %v", err)
	}
	if shouldRetry {
		return fmt.Errorf("download chunk range: retry needed")
	}

	// Upload chunk to new location
	uploadOption := &operation.UploadOption{
		UploadUrl:         dstUrl,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               security.EncodedJwt(assignResult.Auth),
	}
	uploader, err := operation.NewUploader()
	if err != nil {
		return fmt.Errorf("create uploader: %v", err)
	}
	_, err = uploader.UploadData(context.Background(), chunkData, uploadOption)
	if err != nil {
		return fmt.Errorf("upload chunk range: %v", err)
	}

	return nil
}

// min returns the minimum of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two int64 values
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// parseRangeHeader parses the x-amz-copy-source-range header
func parseRangeHeader(rangeHeader string) (startOffset, endOffset int64, err error) {
	// Remove "bytes=" prefix if present
	rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	startOffset, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start offset: %v", err)
	}

	endOffset, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end offset: %v", err)
	}

	return startOffset, endOffset, nil
}

// copyChunksForRange copies chunks that overlap with the specified range
func (s3a *S3ApiServer) copyChunksForRange(entry *filer_pb.Entry, startOffset, endOffset int64, dstPath string) ([]*filer_pb.FileChunk, error) {
	var relevantChunks []*filer_pb.FileChunk

	// Find chunks that overlap with the range
	for _, chunk := range entry.GetChunks() {
		chunkStart := chunk.Offset
		chunkEnd := chunk.Offset + int64(chunk.Size)

		// Check if chunk overlaps with the range
		if chunkStart < endOffset+1 && chunkEnd > startOffset {
			// Calculate the overlap
			overlapStart := max(startOffset, chunkStart)
			overlapEnd := min(endOffset+1, chunkEnd)

			// Create a new chunk with adjusted offset and size relative to the range
			newChunk := &filer_pb.FileChunk{
				FileId:       chunk.FileId,
				Offset:       overlapStart - startOffset, // Offset relative to the range start
				Size:         uint64(overlapEnd - overlapStart),
				ModifiedTsNs: time.Now().UnixNano(),
				ETag:         chunk.ETag,
				IsCompressed: chunk.IsCompressed,
				CipherKey:    chunk.CipherKey,
				Fid:          chunk.Fid,
			}
			relevantChunks = append(relevantChunks, newChunk)
		}
	}

	// Copy the relevant chunks using a specialized method for range copies
	dstChunks := make([]*filer_pb.FileChunk, len(relevantChunks))
	executor := util.NewLimitedConcurrentExecutor(4)
	errChan := make(chan error, len(relevantChunks))

	// Create a map to track original chunks for each relevant chunk
	originalChunks := make([]*filer_pb.FileChunk, len(relevantChunks))
	relevantIndex := 0
	for _, chunk := range entry.GetChunks() {
		chunkStart := chunk.Offset
		chunkEnd := chunk.Offset + int64(chunk.Size)

		// Check if chunk overlaps with the range
		if chunkStart < endOffset+1 && chunkEnd > startOffset {
			originalChunks[relevantIndex] = chunk
			relevantIndex++
		}
	}

	for i, chunk := range relevantChunks {
		chunkIndex := i
		originalChunk := originalChunks[i] // Get the corresponding original chunk
		executor.Execute(func() {
			dstChunk, err := s3a.copySingleChunkForRange(originalChunk, chunk, startOffset, endOffset, dstPath)
			if err != nil {
				errChan <- fmt.Errorf("chunk %d: %v", chunkIndex, err)
				return
			}
			dstChunks[chunkIndex] = dstChunk
			errChan <- nil
		})
	}

	// Wait for all operations to complete and check for errors
	for i := 0; i < len(relevantChunks); i++ {
		if err := <-errChan; err != nil {
			return nil, err
		}
	}

	return dstChunks, nil
}
