package s3api

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
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

	objectLockEnabled, lockErr := s3a.isObjectLockEnabled(bucket)
	if lockErr != nil && !errors.Is(lockErr, filer_pb.ErrNotFound) {
		glog.Errorf("extractZipObject: object lock state for bucket %s: %v", bucket, lockErr)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	// Malformed or unauthorized lock headers fail the request outright, the
	// same as a standalone PutObject; per-member permission re-checks happen in
	// authorizeMember.
	if vErr := s3a.validateObjectLockHeaders(r, bucket, object, objectLockEnabled); vErr != nil {
		s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(vErr))
		return
	}

	// Checksums describing the archive body are verified while it spools, then
	// scrubbed so members are validated against their own bytes instead.
	requestQuery := parseRequestQuery(r)
	archiveMd5 := r.Header.Get("Content-Md5")
	var archiveMd5Hash hash.Hash
	if archiveMd5 != "" {
		archiveMd5Hash = md5.New()
		dataReader = io.TeeReader(dataReader, archiveMd5Hash)
	}
	archiveChecksumAlgo, archiveChecksumHeader, checksumCode := detectRequestedChecksumAlgorithmQ(r, requestQuery)
	if checksumCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, checksumCode)
		return
	}
	var archiveChecksumHash hash.Hash
	expectedArchiveChecksum := ""
	if archiveChecksumAlgo != ChecksumAlgorithmNone {
		if archiveChecksumHash = getCheckSumWriter(archiveChecksumAlgo); archiveChecksumHash != nil {
			dataReader = io.TeeReader(dataReader, archiveChecksumHash)
		}
		expectedArchiveChecksum = lookupHeaderOrQuery(r, requestQuery, archiveChecksumHeader)
	}
	scrubArchiveChecksums(r)
	// Content-Encoding describes the archive body on the wire; the members it
	// fans out to are stored as-is.
	r.Header.Del("Content-Encoding")

	ra, size, cleanup, err := filer.SpoolZipReader(dataReader, 0)
	if err != nil {
		if errors.Is(err, filer.ErrZipTooLarge) {
			s3err.WriteErrorResponse(w, r, s3err.ErrEntityTooLarge)
		} else if strings.Contains(err.Error(), s3err.ErrMsgContentSha256Mismatch) {
			s3err.WriteErrorResponse(w, r, s3err.ErrContentSHA256Mismatch)
		} else {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		return
	}
	defer cleanup()
	if errCode := verifyArchiveChecksums(archiveMd5, archiveMd5Hash, expectedArchiveChecksum, archiveChecksumHash); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}
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

	identity, _ := s3_constants.GetIdentityFromContext(r).(*Identity)
	result := &extractZipResult{}

	join := func(rel string) string {
		if root == "" {
			return rel
		}
		return root + "/" + rel
	}
	// authorizeMember applies to a member key the same checks a standalone
	// PutObject of that key would face: IAM, table layout, bucket policy,
	// object-lock header permissions, and existing-entry protections.
	authorizeMember := func(memberKey string) s3err.ErrorCode {
		if code := s3a.iam.AuthorizeObjectWrite(r, identity, bucket, memberKey); code != s3err.ErrNone {
			return code
		}
		if err := s3a.validateTableBucketObjectPath(bucket, memberKey); err != nil {
			return s3err.ErrAccessDenied
		}
		if code, _ := s3a.checkPolicyWithEntry(r, bucket, memberKey, string(s3_constants.ACTION_WRITE), "", nil); code != s3err.ErrNone {
			return code
		}
		// Lock headers validated for the request still need the dedicated
		// permissions against the member's own key.
		if r.Header.Get(s3_constants.AmzObjectLockMode) != "" || r.Header.Get(s3_constants.AmzObjectLockRetainUntilDate) != "" {
			if !s3a.checkObjectLockHeaderPermission(r, bucket, memberKey, s3_constants.ACTION_PUT_OBJECT_RETENTION) {
				return s3err.ErrAccessDenied
			}
		}
		if r.Header.Get(s3_constants.AmzObjectLockLegalHold) != "" {
			if !s3a.checkObjectLockHeaderPermission(r, bucket, memberKey, s3_constants.ACTION_PUT_OBJECT_LEGAL_HOLD) {
				return s3err.ErrAccessDenied
			}
		}
		if !versioningConfigured {
			if lockErr := s3a.enforceObjectLockProtections(r, bucket, memberKey, "", s3a.evaluateGovernanceBypassRequest(r, bucket, memberKey)); lockErr != nil {
				return s3err.ErrAccessDenied
			}
		}
		return s3err.ErrNone
	}
	writeMember := func(rel string, memberReader io.Reader, memberSize int64) {
		memberKey := join(rel)
		if authCode := authorizeMember(memberKey); authCode != s3err.ErrNone {
			result.Errors = append(result.Errors, memberKey+": "+s3err.GetAPIError(authCode).Code)
			return
		}
		memberContentType := mime.TypeByExtension(path.Ext(memberKey))
		if memberContentType == "" {
			memberContentType = "application/octet-stream"
		}
		// Write helpers read the object from route vars, e.g. for the SSE-KMS
		// encryption context ARN; point them at the member, not the archive.
		memberReq := mux.SetURLVars(r, map[string]string{
			"bucket": bucket,
			"object": memberKey,
		})
		memberReq.Header.Set("Content-Type", memberContentType)
		memberReq.ContentLength = memberSize
		var etag string
		var errCode s3err.ErrorCode
		switch versioningState {
		case s3_constants.VersioningEnabled:
			_, etag, errCode, _ = s3a.putVersionedObject(memberReq, bucket, memberKey, memberReader, memberContentType)
		case s3_constants.VersioningSuspended:
			etag, errCode, _ = s3a.putSuspendedVersioningObject(memberReq, bucket, memberKey, memberReader, memberContentType)
		default:
			filePath := s3a.toFilerPath(bucket, memberKey)
			lifecycleTTLSec := s3a.lifecycleTTLForObjectWrite(bucket, memberKey, memberSize)
			etag, errCode, _ = s3a.putToFiler(memberReq, filePath, memberReader, bucket, memberKey, 1, lifecycleTTLSec, nil, false, "")
		}
		if errCode != s3err.ErrNone {
			result.Errors = append(result.Errors, memberKey+": "+s3err.GetAPIError(errCode).Code)
			return
		}
		result.Extracted = append(result.Extracted, &extractMemberResult{Name: memberKey, Size: memberSize, ETag: etag})
	}
	mkdirMember := func(memberKey string) {
		if authCode := authorizeMember(memberKey); authCode != s3err.ErrNone {
			result.Errors = append(result.Errors, memberKey+": "+s3err.GetAPIError(authCode).Code)
			return
		}
		dirPath := s3a.toFilerPath(bucket, memberKey)
		dirName, entryName := path.Dir(dirPath), path.Base(dirPath)
		// mkdir replaces an existing marker outright, so a lock recorded on the
		// entry itself is checked under the same lock the replacement takes,
		// mirroring the explicit-directory branch of PutObjectHandler.
		markerCode := s3a.withObjectWriteLock(bucket, memberKey, func() s3err.ErrorCode {
			if !objectLockEnabled {
				return s3err.ErrNone
			}
			existing, existErr := s3a.getEntry(dirName, entryName)
			if existErr != nil {
				return s3err.ErrNone
			}
			if lockErr := s3a.enforceObjectLockOnEntry(existing, bucket, memberKey, "", s3a.evaluateGovernanceBypassRequest(r, bucket, memberKey)); lockErr != nil {
				return s3err.ErrAccessDenied
			}
			return s3err.ErrNone
		}, func() s3err.ErrorCode {
			mkErr := s3a.mkdir(dirName, entryName, func(entry *filer_pb.Entry) {
				entry.Attributes.Mime = s3_constants.FolderMimeType
				s3a.setObjectOwnerFromRequest(r, bucket, entry)
				applyPutObjectACL(r, entry)
				if lockErr := s3a.extractObjectLockMetadataFromRequest(r, entry); lockErr != nil {
					glog.Errorf("extractZipObject: object lock metadata for %s/%s: %v", bucket, memberKey, lockErr)
				}
			})
			return filerErrorToS3Error(mkErr)
		})
		if markerCode != s3err.ErrNone {
			result.Errors = append(result.Errors, memberKey+": "+s3err.GetAPIError(markerCode).Code)
		}
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
			mkdirMember(join(rel))
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

// verifyArchiveChecksums compares the archive's supplied Content-Md5 and
// x-amz-checksum-* values against the bytes consumed while spooling. A nil
// hash means the client supplied nothing to check.
func verifyArchiveChecksums(expectedMd5 string, md5Hash hash.Hash, expectedChecksum string, checksumHash hash.Hash) s3err.ErrorCode {
	if md5Hash != nil {
		expected, err := base64.StdEncoding.DecodeString(expectedMd5)
		if err != nil {
			return s3err.ErrInvalidDigest
		}
		if sum := md5Hash.Sum(nil); !bytes.Equal(expected, sum) {
			return s3err.ErrBadDigest
		}
	}
	if checksumHash != nil && expectedChecksum != "" {
		if sum := base64.StdEncoding.EncodeToString(checksumHash.Sum(nil)); expectedChecksum != sum {
			return s3err.ErrBadDigest
		}
	}
	return s3err.ErrNone
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
