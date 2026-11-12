package s3api

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

var (
	errRequestBodyTooLarge   = errors.New("request body too large")
	errContentSha256Mismatch = errors.New(s3err.ErrMsgContentSha256Mismatch)
)

func readRequestBody(r *http.Request, limit int64) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errRequestBodyTooLarge
	}
	return body, nil
}

// signedPayloadBody hands out a signed request body only after all of it has
// been read and matched against the payload hash the signature covered, so a
// handler that stops reading early never acts on unverified bytes.
type signedPayloadBody struct {
	body   io.ReadCloser
	size   int64
	limit  int64
	sha256 []byte
	data   *bytes.Reader
	err    error
}

func (b *signedPayloadBody) Read(p []byte) (int, error) {
	if b.data == nil && b.err == nil {
		b.data, b.err = b.load()
	}
	if b.err != nil {
		return 0, b.err
	}
	return b.data.Read(p)
}

func (b *signedPayloadBody) load() (*bytes.Reader, error) {
	if b.size > b.limit {
		return nil, errRequestBodyTooLarge
	}
	data, err := io.ReadAll(io.LimitReader(b.body, b.limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > b.limit {
		return nil, errRequestBodyTooLarge
	}
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], b.sha256) {
		return nil, errContentSha256Mismatch
	}
	return bytes.NewReader(data), nil
}

func (b *signedPayloadBody) Close() error {
	return b.body.Close()
}

// bindSignedPayload ties the request body to hashedPayload, the payload hash a
// V4 signature covered. Uploads read through getRequestDataReader, which
// verifies the body as it streams instead.
func bindSignedPayload(r *http.Request, hashedPayload string) s3err.ErrorCode {
	if _, bound := r.Body.(*signedPayloadBody); bound {
		return s3err.ErrNone
	}
	expected, valid := decodeContentSha256(hashedPayload)
	hasBody := r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0
	if !valid {
		if hasBody {
			return s3err.ErrInvalidArgument
		}
		return s3err.ErrNone
	}
	if expected == nil {
		return s3err.ErrNone
	}
	if !hasBody {
		if emptySum := sha256.Sum256(nil); !bytes.Equal(expected, emptySum[:]) {
			return s3err.ErrContentSHA256Mismatch
		}
		return s3err.ErrNone
	}
	r.Body = &signedPayloadBody{
		body:   r.Body,
		size:   r.ContentLength,
		limit:  s3_constants.GetRequestBodyLimitFromContext(r, iamRequestBodyLimit),
		sha256: expected,
	}
	return s3err.ErrNone
}
