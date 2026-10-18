package s3api

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// getRequestDataReader returns the appropriate reader for the request body.
// Chunked streaming uploads are decoded regardless of whether IAM is enabled;
// with no credentials to verify against, signature checking is skipped inside
// newChunkedReader.
func getRequestDataReader(s3a *S3ApiServer, r *http.Request) (io.ReadCloser, s3err.ErrorCode) {
	var s3ErrCode s3err.ErrorCode
	dataReader := r.Body
	rAuthType := getRequestAuthType(r)
	if rAuthType == authTypeStreamingSigned || rAuthType == authTypeStreamingUnsigned {
		dataReader, s3ErrCode = s3a.iam.newChunkedReader(r)
	}
	if s3ErrCode != s3err.ErrNone {
		return nil, s3ErrCode
	}

	expected, valid := expectedContentSha256(r)
	if !valid {
		return nil, s3err.ErrInvalidArgument
	}
	if expected != nil && dataReader != nil {
		if r.ContentLength == 0 {
			// Handlers may never read an empty body, so check it now.
			emptySum := sha256.Sum256(nil)
			if !bytes.Equal(expected, emptySum[:]) {
				return nil, s3err.ErrContentSHA256Mismatch
			}
			return dataReader, s3err.ErrNone
		}
		dataReader = &contentSha256Verifier{reader: dataReader, hasher: sha256.New(), expected: expected}
	}
	return dataReader, s3err.ErrNone
}

// expectedContentSha256 decodes the x-amz-content-sha256 header when it declares
// a payload hash — hex or base64 per SigV4 — so the body can be checked as it
// streams. Sentinel values (streaming, unsigned) carry no hash and are skipped;
// anything that decodes to neither returns valid=false.
func expectedContentSha256(r *http.Request) (expected []byte, valid bool) {
	v := r.Header.Get("X-Amz-Content-Sha256")
	if v == "" || v == unsignedPayload || v == streamingContentSHA256 ||
		v == streamingContentSHA256Trailer || v == streamingUnsignedPayload {
		return nil, true
	}
	if decoded, err := hex.DecodeString(v); err == nil && len(decoded) == sha256.Size {
		return decoded, true
	}
	if decoded, err := base64.StdEncoding.DecodeString(v); err == nil && len(decoded) == sha256.Size {
		return decoded, true
	}
	return nil, false
}

// contentSha256Verifier checks the decoded x-amz-content-sha256 once the stream
// is exhausted; handlers that never consume the body are unaffected.
type contentSha256Verifier struct {
	reader   io.ReadCloser
	hasher   hash.Hash
	expected []byte
	done     bool
	pending  error
}

func (v *contentSha256Verifier) Read(p []byte) (int, error) {
	if v.pending != nil {
		return 0, v.pending
	}
	n, err := v.reader.Read(p)
	v.hasher.Write(p[:n])
	if err == io.EOF && !v.done {
		v.done = true
		if !bytes.Equal(v.hasher.Sum(nil), v.expected) {
			v.pending = errors.New(s3err.ErrMsgContentSha256Mismatch)
			err = nil
		}
	}
	if n == 0 && v.pending != nil {
		return 0, v.pending
	}
	return n, err
}

func (v *contentSha256Verifier) Close() error {
	return v.reader.Close()
}
