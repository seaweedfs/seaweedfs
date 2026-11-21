package s3api

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

var (
	errRequestBodyTooLarge   = errors.New("request body too large")
	errContentSha256Mismatch = errors.New(s3err.ErrMsgContentSha256Mismatch)
	errContentMd5Mismatch    = errors.New("the Content-MD5 you specified did not match what was received")
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
// been read and matched against the payload hash the signature covered and
// any Content-MD5, so a handler that stops reading early never acts on
// unverified bytes.
type signedPayloadBody struct {
	body   io.ReadCloser
	size   int64
	limit  int64
	sha256 []byte
	md5    []byte
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
	if b.sha256 != nil {
		if sum := sha256.Sum256(data); !bytes.Equal(sum[:], b.sha256) {
			return nil, errContentSha256Mismatch
		}
	}
	if b.md5 != nil {
		if sum := md5.Sum(data); !bytes.Equal(sum[:], b.md5) {
			return nil, errContentMd5Mismatch
		}
	}
	return bytes.NewReader(data), nil
}

func (b *signedPayloadBody) Close() error {
	return b.body.Close()
}

func (b *signedPayloadBody) wrap(body *io.ReadCloser) {
	b.body = *body
	*body = b
}

// unwrapSignedPayload puts back the raw body of a signedPayloadBody nothing has
// read yet, for readers that verify it as it streams, and returns the hash it
// was bound to.
func unwrapSignedPayload(body *io.ReadCloser) []byte {
	if b, ok := (*body).(*signedPayloadBody); ok && b.data == nil && b.err == nil {
		*body = b.body
		return b.sha256
	}
	return nil
}

// bindSignedPayload ties the request body to hashedPayload, the payload hash a
// V4 signature covered (UNSIGNED-PAYLOAD for V2), and to any Content-MD5.
// Uploads read through getRequestDataReader, which verifies the body as it
// streams instead.
func bindSignedPayload(r *http.Request, hashedPayload string) s3err.ErrorCode {
	if bound, ok := r.Body.(*signedPayloadBody); ok {
		if bound.sha256 != nil || bound.md5 != nil {
			return s3err.ErrNone
		}
		// streamHashRequestBody buffered it and signed its hash; only
		// Content-MD5 is left to check.
		hashedPayload = unsignedPayload
	}
	switch hashedPayload {
	case streamingContentSHA256, streamingContentSHA256Trailer, streamingUnsignedPayload:
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
	var contentMd5 []byte
	if vals := r.Header.Values("Content-Md5"); len(vals) > 0 {
		if fips140Enforced() {
			return s3err.ErrNotImplemented
		}
		if len(vals) != 1 {
			return s3err.ErrInvalidDigest
		}
		decoded, err := validateContentMd5(r.Header)
		if err != nil || len(decoded) != md5.Size || base64.StdEncoding.EncodeToString(decoded) != vals[0] {
			return s3err.ErrInvalidDigest
		}
		contentMd5 = decoded
	}
	if expected == nil && contentMd5 == nil {
		return s3err.ErrNone
	}
	if !hasBody {
		if emptySum := sha256.Sum256(nil); expected != nil && !bytes.Equal(expected, emptySum[:]) {
			return s3err.ErrContentSHA256Mismatch
		}
		if emptySum := md5.Sum(nil); contentMd5 != nil && !bytes.Equal(contentMd5, emptySum[:]) {
			return s3err.ErrBadDigest
		}
		return s3err.ErrNone
	}
	signed := &signedPayloadBody{
		size:   r.ContentLength,
		limit:  s3_constants.GetRequestBodyLimitFromContext(r, iamRequestBodyLimit),
		sha256: expected,
		md5:    contentMd5,
	}
	signed.wrap(&r.Body)
	return s3err.ErrNone
}

// requestBodyErrorCode names the check a failed body read tripped, or returns
// fallback for any other read error.
func requestBodyErrorCode(err error, fallback s3err.ErrorCode) s3err.ErrorCode {
	switch {
	case errors.Is(err, errContentSha256Mismatch):
		return s3err.ErrContentSHA256Mismatch
	case errors.Is(err, errContentMd5Mismatch):
		return s3err.ErrBadDigest
	case errors.Is(err, errRequestBodyTooLarge):
		return s3err.ErrEntityTooLarge
	}
	return fallback
}

type requestBodyChecksum struct {
	md5              []byte
	checksumAlg      ChecksumAlgorithm
	checksumExpected []byte
}

func requestHeaderOrQueryValues(r *http.Request, query url.Values, canonicalHeader string) []string {
	vals := append([]string(nil), r.Header.Values(canonicalHeader)...)
	for k, v := range query {
		if strings.EqualFold(k, canonicalHeader) {
			vals = append(vals, v...)
		}
	}
	return vals
}

// parseRequestBodyChecksum parses Content-MD5 and x-amz-checksum-* headers/query
// parameters for buffered XML request handlers.
func parseRequestBodyChecksum(r *http.Request) (requestBodyChecksum, s3err.ErrorCode) {
	var check requestBodyChecksum

	if vals := r.Header.Values("Content-Md5"); len(vals) > 0 {
		if fips140Enforced() {
			return requestBodyChecksum{}, s3err.ErrNotImplemented
		}
		if len(vals) != 1 {
			return requestBodyChecksum{}, s3err.ErrInvalidDigest
		}
		if bound, ok := r.Body.(*signedPayloadBody); !ok || bound.md5 == nil {
			if vals[0] == "" {
				return requestBodyChecksum{}, s3err.ErrInvalidDigest
			}
			decoded, err := base64.StdEncoding.DecodeString(vals[0])
			if err != nil || len(decoded) != md5.Size || base64.StdEncoding.EncodeToString(decoded) != vals[0] {
				return requestBodyChecksum{}, s3err.ErrInvalidDigest
			}
			check.md5 = decoded
		}
	}

	var query url.Values
	if r.URL != nil && r.URL.RawQuery != "" {
		var err error
		query, err = url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
	}

	if trailerVals := requestHeaderOrQueryValues(r, query, s3_constants.AmzTrailer); len(trailerVals) > 0 {
		if fips140Enforced() {
			for _, v := range trailerVals {
				for _, part := range strings.Split(v, ",") {
					if m, ok := trailerToChecksumAlgorithm[strings.ToLower(strings.TrimSpace(part))]; ok && m.alg == ChecksumAlgorithmSHA1 {
						return requestBodyChecksum{}, s3err.ErrNotImplemented
					}
				}
			}
		}
		// Buffered XML handlers do not decode aws-chunked trailers and must not ignore declared trailer checksums.
		return requestBodyChecksum{}, s3err.ErrInvalidRequest
	}

	for _, v := range requestHeaderOrQueryValues(r, query, "X-Amz-Content-Sha256") {
		switch v {
		case streamingContentSHA256, streamingContentSHA256Trailer, streamingUnsignedPayload:
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
	}
	for _, v := range r.Header.Values("Content-Encoding") {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "aws-chunked") {
				return requestBodyChecksum{}, s3err.ErrInvalidRequest
			}
		}
	}

	declaredAlg := ChecksumAlgorithmNone
	for _, header := range []string{s3_constants.AmzSdkChecksumAlgorithm, s3_constants.AmzChecksumAlgorithm} {
		vals := requestHeaderOrQueryValues(r, query, header)
		if len(vals) == 0 {
			continue
		}
		if len(vals) != 1 || vals[0] == "" {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
		m, ok := checksumAlgorithmMapping[strings.ToUpper(vals[0])]
		if !ok {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
		if declaredAlg != ChecksumAlgorithmNone && declaredAlg != m.alg {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
		declaredAlg = m.alg
	}

	valueAlg := ChecksumAlgorithmNone
	var rawValue string
	for _, entry := range checksumHeaders {
		vals := requestHeaderOrQueryValues(r, query, entry.header)
		if len(vals) == 0 {
			continue
		}
		if len(vals) > 1 || vals[0] == "" {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
		if valueAlg != ChecksumAlgorithmNone {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
		valueAlg = entry.alg
		rawValue = vals[0]
	}

	if (declaredAlg == ChecksumAlgorithmSHA1 || valueAlg == ChecksumAlgorithmSHA1) && fips140Enforced() {
		return requestBodyChecksum{}, s3err.ErrNotImplemented
	}
	if declaredAlg != ChecksumAlgorithmNone && valueAlg != ChecksumAlgorithmNone && declaredAlg != valueAlg {
		return requestBodyChecksum{}, s3err.ErrBadDigest
	}
	if declaredAlg != ChecksumAlgorithmNone && valueAlg == ChecksumAlgorithmNone {
		return requestBodyChecksum{}, s3err.ErrInvalidRequest
	}

	if valueAlg != ChecksumAlgorithmNone {
		hasher := getCheckSumWriter(valueAlg)
		decoded, err := base64.StdEncoding.DecodeString(rawValue)
		if err != nil || len(decoded) != hasher.Size() || base64.StdEncoding.EncodeToString(decoded) != rawValue {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
		check.checksumAlg = valueAlg
		check.checksumExpected = decoded
	}

	return check, s3err.ErrNone
}

func (c requestBodyChecksum) verify(body []byte) s3err.ErrorCode {
	if c.md5 != nil {
		sum := md5.Sum(body)
		if !bytes.Equal(sum[:], c.md5) {
			return s3err.ErrBadDigest
		}
	}
	if c.checksumAlg != ChecksumAlgorithmNone {
		h := getCheckSumWriter(c.checksumAlg)
		h.Write(body)
		if !bytes.Equal(h.Sum(nil), c.checksumExpected) {
			return s3err.ErrBadDigest
		}
	}
	return s3err.ErrNone
}
