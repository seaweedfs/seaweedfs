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
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// getRequestDataReader returns the appropriate reader for the request body.
// Chunked streaming uploads are decoded regardless of whether IAM is enabled;
// with no credentials to verify against, signature checking is skipped inside
// newChunkedReader.
func getRequestDataReader(s3a *S3ApiServer, r *http.Request) (io.ReadCloser, s3err.ErrorCode) {
	var s3ErrCode s3err.ErrorCode
	signedSha256 := unwrapSignedPayload(&r.Body)
	dataReader := r.Body
	if isAwsChunkedUpload(r) {
		dataReader, s3ErrCode = s3a.iam.newChunkedReader(r)
		if s3ErrCode != s3err.ErrNone {
			return nil, s3ErrCode
		}
		if decoded, ok := decodedContentLength(r); ok {
			dataReader = &decodedLengthLimiter{reader: dataReader, remaining: decoded}
		}
	}

	expected, valid := expectedContentSha256(r)
	if !valid {
		return nil, s3err.ErrInvalidArgument
	}
	if signedSha256 != nil {
		expected = signedSha256
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

// isAwsChunkedUpload reports whether r's body is aws-chunked encoded, the
// streaming forms getRequestDataReader decodes.
func isAwsChunkedUpload(r *http.Request) bool {
	authType := getRequestAuthType(r)
	return authType == authTypeStreamingSigned || authType == authTypeStreamingUnsigned
}

// errDecodedLengthExceeded: an aws-chunked body decoded to more bytes than its
// X-Amz-Decoded-Content-Length declared.
var errDecodedLengthExceeded = errors.New("aws-chunked body is longer than its X-Amz-Decoded-Content-Length")

// decodedContentLength is the X-Amz-Decoded-Content-Length an aws-chunked
// request declares, when it carries a valid one.
func decodedContentLength(r *http.Request) (int64, bool) {
	v := r.Header.Get("X-Amz-Decoded-Content-Length")
	if v == "" {
		return 0, false
	}
	decoded, err := strconv.ParseInt(v, 10, 64)
	if err != nil || decoded < 0 {
		return 0, false
	}
	return decoded, true
}

// decodedLengthLimiter stops an aws-chunked body at its declared decoded
// length. The upload budget charges a request of unknown Content-Length by that
// header, so a body that ran past it would hold bytes nobody reserved; it fails
// instead.
type decodedLengthLimiter struct {
	reader    io.ReadCloser
	remaining int64
}

func (l *decodedLengthLimiter) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		// Everything declared has been read: the body must end here.
		var probe [1]byte
		n, err := l.reader.Read(probe[:])
		if n > 0 {
			return 0, errDecodedLengthExceeded
		}
		return 0, err
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.reader.Read(p)
	l.remaining -= int64(n)
	return n, err
}

func (l *decodedLengthLimiter) Close() error {
	return l.reader.Close()
}

// expectedContentSha256 decodes the x-amz-content-sha256 header when it declares
// a payload hash — hex or base64 per SigV4 — so the body can be checked as it
// streams. Sentinel values (streaming, unsigned) carry no hash and are skipped;
// anything that decodes to neither returns valid=false.
func expectedContentSha256(r *http.Request) (expected []byte, valid bool) {
	return decodeContentSha256(r.Header.Get("X-Amz-Content-Sha256"))
}

func decodeContentSha256(v string) (expected []byte, valid bool) {
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
