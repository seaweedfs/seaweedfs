package s3api

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func isExtractZip(r *http.Request) bool {
	if r.URL.Query().Get("extract") == "zip" ||
		r.Header.Get(s3_constants.SeaweedFSExtract) == "zip" {
		return true
	}
	// AWS Snowball auto-extract: header presence requests extraction.
	if v := r.Header.Get(s3_constants.AmzMetaSnowballAutoExtract); v != "" && v != "false" {
		return true
	}
	return false
}

func hasExplicitSseHeaders(r *http.Request) bool {
	return r.Header.Get(s3_constants.AmzServerSideEncryption) != "" ||
		r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId) != "" ||
		r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != ""
}

type extractMemberResult struct {
	Name string `json:"name"`
	Size int64  `json:"size,omitempty"`
	ETag string `json:"etag,omitempty"`
}

type extractZipResult struct {
	Extracted []*extractMemberResult `json:"extracted"`
	Errors    []string               `json:"errors,omitempty"`
}

// extractZipObject writes a PutObject zip body as one object per member under
// the object's directory, like AWS Snowball's x-amz-meta-snowball-auto-extract.
// The archive object itself is not stored.
func (s3a *S3ApiServer) extractZipObject(w http.ResponseWriter, r *http.Request, bucket, object string, dataReader io.Reader) {
	if hasExplicitSseHeaders(r) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	versioningState, err := s3a.getVersioningState(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			if !s3a.handleAutoCreateBucket(w, r, bucket, "PutObjectHandler") {
				return
			}
			versioningState, err = s3a.getVersioningState(bucket)
		}
		if err != nil {
			glog.Errorf("extractZipObject: versioning state for bucket %s: %v", bucket, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	}
	versioningConfigured := versioningState != ""

	r, aclCode := s3a.preparePutObjectACL(r, bucket)
	if aclCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, aclCode)
		return
	}
	scrubArchiveChecksums(r)

	ra, size, cleanup, err := filer.SpoolZipReader(dataReader)
	if err != nil {
		if strings.Contains(err.Error(), s3err.ErrMsgContentSha256Mismatch) {
			s3err.WriteErrorResponse(w, r, s3err.ErrContentSHA256Mismatch)
		} else {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}
	defer cleanup()
	zr, err := zip.NewReader(ra, size)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	if err = filer.CheckZipLimits(zr); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	root := object
	if !strings.HasSuffix(root, "/") {
		root = path.Dir(root)
	}
	root = strings.TrimSuffix(root, "/")
	if root == "." {
		root = ""
	}

	governanceBypass := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
	result := &extractZipResult{}

	join := func(rel string) string {
		if root == "" {
			return rel
		}
		return root + "/" + rel
	}
	writeMember := func(rel string, memberReader io.Reader, memberSize int64) {
		memberKey := join(rel)
		if !versioningConfigured {
			if lockErr := s3a.enforceObjectLockProtections(r, bucket, memberKey, "", governanceBypass); lockErr != nil {
				result.Errors = append(result.Errors, memberKey+": "+s3err.GetAPIError(s3err.ErrAccessDenied).Code)
				return
			}
		}
		if code, _ := s3a.checkPolicyWithEntry(r, bucket, memberKey, string(s3_constants.ACTION_WRITE), "", nil); code != s3err.ErrNone {
			result.Errors = append(result.Errors, memberKey+": "+s3err.GetAPIError(code).Code)
			return
		}
		memberContentType := mime.TypeByExtension(path.Ext(memberKey))
		if memberContentType == "" {
			memberContentType = "application/octet-stream"
		}
		r.Header.Set("Content-Type", memberContentType)
		r.ContentLength = memberSize
		var etag string
		var errCode s3err.ErrorCode
		switch versioningState {
		case s3_constants.VersioningEnabled:
			_, etag, errCode, _ = s3a.putVersionedObject(r, bucket, memberKey, memberReader, memberContentType)
		case s3_constants.VersioningSuspended:
			etag, errCode, _ = s3a.putSuspendedVersioningObject(r, bucket, memberKey, memberReader, memberContentType)
		default:
			filePath := s3a.toFilerPath(bucket, memberKey)
			etag, errCode, _ = s3a.putToFiler(r, filePath, memberReader, bucket, memberKey, 1, 0, nil, false, "")
		}
		if errCode != s3err.ErrNone {
			result.Errors = append(result.Errors, memberKey+": "+s3err.GetAPIError(errCode).Code)
			return
		}
		result.Extracted = append(result.Extracted, &extractMemberResult{Name: memberKey, Size: memberSize, ETag: etag})
	}

	for _, zf := range zr.File {
		if ctxErr := r.Context().Err(); ctxErr != nil {
			result.Errors = append(result.Errors, ctxErr.Error())
			break
		}
		rel, ok := filer.SanitizeZipMemberName(zf)
		if !ok {
			result.Errors = append(result.Errors, zf.Name+": unsafe member name")
			continue
		}
		if zf.FileInfo().IsDir() {
			dirPath := s3a.toFilerPath(bucket, join(rel))
			mkErr := s3a.mkdir(path.Dir(dirPath), path.Base(dirPath), func(entry *filer_pb.Entry) {
				entry.Attributes.Mime = s3_constants.FolderMimeType
				s3a.setObjectOwnerFromRequest(r, bucket, entry)
				applyPutObjectACL(r, entry)
			})
			if mkErr != nil {
				result.Errors = append(result.Errors, join(rel)+": "+mkErr.Error())
			}
			continue
		}
		memberReader, openErr := zf.Open()
		if openErr != nil {
			result.Errors = append(result.Errors, rel+": "+openErr.Error())
			continue
		}
		writeMember(rel, memberReader, int64(zf.UncompressedSize64))
		memberReader.Close()
	}

	w.Header().Set(s3_constants.SeaweedFSExtractedCount, strconv.Itoa(len(result.Extracted)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

// scrubArchiveChecksums removes request checksums that describe the archive
// body; members are validated against their own bytes.
func scrubArchiveChecksums(r *http.Request) {
	isChecksumHeader := func(k string) bool {
		k = strings.ToLower(k)
		return k == "content-md5" || k == "x-amz-trailer" || k == "x-amz-content-sha256" ||
			strings.HasPrefix(k, "x-amz-checksum") || strings.HasPrefix(k, "x-amz-sdk-checksum")
	}
	for k := range r.Header {
		if isChecksumHeader(k) {
			delete(r.Header, k)
		}
	}
	query := r.URL.Query()
	for k := range query {
		if isChecksumHeader(k) {
			query.Del(k)
		}
	}
	r.URL.RawQuery = query.Encode()
}
