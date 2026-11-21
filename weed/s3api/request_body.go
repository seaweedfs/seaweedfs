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
	contentMd5, errCode := parseCanonicalContentMd5(r.Header)
	if errCode != s3err.ErrNone {
		return errCode
	}
	if expected == nil && contentMd5 == nil {
		return s3err.ErrNone
	}
	if !hasBody {
		if emptySum := sha256.Sum256(nil); expected != nil && !bytes.Equal(expected, emptySum[:]) {
			return s3err.ErrContentSHA256Mismatch
		}
		if contentMd5 != nil {
			if emptySum := md5.Sum(nil); !bytes.Equal(contentMd5, emptySum[:]) {
				return s3err.ErrBadDigest
			}
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

// parseCanonicalContentMd5 decodes the single Content-MD5 header, rejecting a
// base64 spelling that does not re-encode to the sent text so the digest covers
// the bytes the signature covered.
func parseCanonicalContentMd5(h http.Header) ([]byte, s3err.ErrorCode) {
	vals := h.Values("Content-Md5")
	if len(vals) == 0 {
		return nil, s3err.ErrNone
	}
	if len(vals) != 1 || vals[0] == "" {
		return nil, s3err.ErrInvalidDigest
	}
	decoded, err := base64.StdEncoding.DecodeString(vals[0])
	if err != nil || len(decoded) != md5.Size || base64.StdEncoding.EncodeToString(decoded) != vals[0] {
		return nil, s3err.ErrInvalidDigest
	}
	// Under fips140=only the stored digest is not an MD5, so the header's
	// syntax is still enforced but its value goes unchecked.
	if fips140Enforced() {
		return nil, s3err.ErrNone
	}
	return decoded, s3err.ErrNone
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

func requestHeaderOrQueryValues(r *http.Request, lowerQuery map[string][]string, canonicalHeader string) []string {
	headerVals := r.Header.Values(canonicalHeader)
	queryVals := lowerQuery[strings.ToLower(canonicalHeader)]
	switch {
	case len(queryVals) == 0:
		return headerVals
	case len(headerVals) == 0:
		return queryVals
	}
	vals := make([]string, 0, len(headerVals)+len(queryVals))
	vals = append(vals, headerVals...)
	return append(vals, queryVals...)
}

// parseRequestBodyChecksum parses Content-MD5 and x-amz-checksum-* headers/query
// parameters for buffered XML request handlers.
func parseRequestBodyChecksum(r *http.Request) (requestBodyChecksum, s3err.ErrorCode) {
	var check requestBodyChecksum

	decoded, errCode := parseCanonicalContentMd5(r.Header)
	if errCode != s3err.ErrNone {
		return requestBodyChecksum{}, errCode
	}
	if bound, ok := r.Body.(*signedPayloadBody); !ok || bound.md5 == nil {
		check.md5 = decoded
	}

	var lowerQuery map[string][]string
	if r.URL != nil && r.URL.RawQuery != "" {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			return requestBodyChecksum{}, s3err.ErrInvalidRequest
		}
		lowerQuery = make(map[string][]string, len(query))
		for k, v := range query {
			lk := strings.ToLower(k)
			lowerQuery[lk] = append(lowerQuery[lk], v...)
		}
	}

	if trailerVals := requestHeaderOrQueryValues(r, lowerQuery, s3_constants.AmzTrailer); len(trailerVals) > 0 {
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

	for _, v := range requestHeaderOrQueryValues(r, lowerQuery, "X-Amz-Content-Sha256") {
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
		vals := requestHeaderOrQueryValues(r, lowerQuery, header)
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
		vals := requestHeaderOrQueryValues(r, lowerQuery, entry.header)
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
