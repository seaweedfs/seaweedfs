package s3api

// the related code is copied and modified from minio source code

/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"github.com/dustin/go-humanize"
	"github.com/minio/crc64nvme"
)

// calculateSeedSignature - Calculate seed signature in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
//
// returns signature, error otherwise if the signature mismatches or any other
// error while parsing and validating.
func (iam *IdentityAccessManagement) calculateSeedSignature(r *http.Request) (cred *Credential, signature string, region string, date time.Time, errCode s3err.ErrorCode) {

	// Copy request.
	req := *r

	// Save authorization header.
	v4Auth := req.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, errCode := parseSignV4(v4Auth)
	if errCode != s3err.ErrNone {
		return nil, "", "", time.Time{}, errCode
	}

	contentSha256Header := req.Header.Get("X-Amz-Content-Sha256")

	switch contentSha256Header {
	// Payload for STREAMING signature should be 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'
	case streamingContentSHA256:
		glog.V(3).Infof("streaming content sha256")
	case streamingUnsignedPayload:
		glog.V(3).Infof("streaming unsigned payload")
	default:
		return nil, "", "", time.Time{}, s3err.ErrContentSHA256Mismatch
	}

	// Payload streaming.
	payload := contentSha256Header

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(signV4Values.SignedHeaders, r)
	if errCode != s3err.ErrNone {
		return nil, "", "", time.Time{}, errCode
	}
	// Verify if the access key id matches.
	identity, cred, found := iam.lookupByAccessKey(signV4Values.Credential.accessKey)
	if !found {
		return nil, "", "", time.Time{}, s3err.ErrInvalidAccessKeyID
	}

	bucket, object := s3_constants.GetBucketAndObject(r)
	if !identity.canDo(s3_constants.ACTION_WRITE, bucket, object) {
		errCode = s3err.ErrAccessDenied
		return
	}

	// Verify if region is valid.
	region = signV4Values.Credential.scope.region

	// Extract date, if not present throw error.
	var dateStr string
	if dateStr = req.Header.Get(http.CanonicalHeaderKey("x-amz-date")); dateStr == "" {
		if dateStr = r.Header.Get("Date"); dateStr == "" {
			return nil, "", "", time.Time{}, s3err.ErrMissingDateHeader
		}
	}
	// Parse date header.
	var err error
	date, err = time.Parse(iso8601Format, dateStr)
	if err != nil {
		return nil, "", "", time.Time{}, s3err.ErrMalformedDate
	}

	// Query string.
	queryStr := req.URL.Query().Encode()

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, payload, queryStr, req.URL.Path, req.Method)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, date, signV4Values.Credential.getScope())

	// Calculate signature.
	newSignature := iam.getSignature(
		cred.SecretKey,
		signV4Values.Credential.scope.date,
		region,
		"s3",
		stringToSign,
	)

	// Verify if signature match.
	if !compareSignatureV4(newSignature, signV4Values.Signature) {
		return nil, "", "", time.Time{}, s3err.ErrSignatureDoesNotMatch
	}

	// Return calculated signature.
	return cred, newSignature, region, date, s3err.ErrNone
}

const maxLineLength = 4 * humanize.KiByte // assumed <= bufio.defaultBufSize 4KiB

// lineTooLong is generated as chunk header is bigger than 4KiB.
var errLineTooLong = errors.New("header line too long")

// Malformed encoding is generated when chunk header is wrongly formed.
var errMalformedEncoding = errors.New("malformed chunked encoding")

// newChunkedReader returns a new s3ChunkedReader that translates the data read from r
// out of HTTP "chunked" format before returning it.
// The s3ChunkedReader returns io.EOF when the final 0-length chunk is read.
func (iam *IdentityAccessManagement) newChunkedReader(req *http.Request) (io.ReadCloser, s3err.ErrorCode) {
	glog.V(3).Infof("creating a new newSignV4ChunkedReader")

	contentSha256Header := req.Header.Get("X-Amz-Content-Sha256")
	authorizationHeader := req.Header.Get("Authorization")

	var ident *Credential
	var seedSignature, region string
	var seedDate time.Time
	var errCode s3err.ErrorCode

	switch contentSha256Header {
	// Payload for STREAMING signature should be 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'
	case streamingContentSHA256:
		glog.V(3).Infof("streaming content sha256")
		ident, seedSignature, region, seedDate, errCode = iam.calculateSeedSignature(req)
		if errCode != s3err.ErrNone {
			return nil, errCode
		}
	case streamingUnsignedPayload:
		glog.V(3).Infof("streaming unsigned payload")
		if authorizationHeader != "" {
			// We do not need to pass the seed signature to the Reader as each chunk is not signed,
			// but we do compute it to verify the caller has the correct permissions.
			_, _, _, _, errCode = iam.calculateSeedSignature(req)
			if errCode != s3err.ErrNone {
				return nil, errCode
			}
		}
	}

	// Get the checksum algorithm from the x-amz-trailer Header.
	amzTrailerHeader := req.Header.Get("x-amz-trailer")
	checksumAlgorithm, err := extractChecksumAlgorithm(amzTrailerHeader)

	if err != nil {
		glog.V(3).Infof("error extracting checksum algorithm: %v", err)
		return nil, s3err.ErrInvalidRequest
	}

	checkSumWriter := getCheckSumWriter(checksumAlgorithm)

	return &s3ChunkedReader{
		cred:              ident,
		reader:            bufio.NewReader(req.Body),
		seedSignature:     seedSignature,
		seedDate:          seedDate,
		region:            region,
		chunkSHA256Writer: sha256.New(),
		checkSumAlgorithm: checksumAlgorithm.String(),
		checkSumWriter:    checkSumWriter,
		state:             readChunkHeader,
		iam:               iam,
	}, s3err.ErrNone
}

func extractChecksumAlgorithm(amzTrailerHeader string) (ChecksumAlgorithm, error) {
	// Extract checksum algorithm from the x-amz-trailer header.
	switch amzTrailerHeader {
	case "x-amz-checksum-crc32":
		return ChecksumAlgorithmCRC32, nil
	case "x-amz-checksum-crc32c":
		return ChecksumAlgorithmCRC32C, nil
	case "x-amz-checksum-crc64nvme":
		return ChecksumAlgorithmCRC64NVMe, nil
	case "x-amz-checksum-sha1":
		return ChecksumAlgorithmSHA1, nil
	case "x-amz-checksum-sha256":
		return ChecksumAlgorithmSHA256, nil
	case "":
		return ChecksumAlgorithmNone, nil
	default:
		return ChecksumAlgorithmNone, errors.New("unsupported checksum algorithm '" + amzTrailerHeader + "'")
	}
}

// Represents the overall state that is required for decoding a
// AWS Signature V4 chunked reader.
type s3ChunkedReader struct {
	cred              *Credential
	reader            *bufio.Reader
	seedSignature     string
	seedDate          time.Time
	region            string
	state             chunkState
	lastChunk         bool
	chunkSignature    string // Empty string if unsigned streaming upload.
	checkSumAlgorithm string // Empty string if no checksum algorithm is specified.
	checkSumWriter    hash.Hash
	chunkSHA256Writer hash.Hash // Calculates sha256 of chunk data.
	n                 uint64    // Unread bytes in chunk
	err               error
	iam               *IdentityAccessManagement
}

// Read chunk reads the chunk token signature portion.
func (cr *s3ChunkedReader) readS3ChunkHeader() {
	// Read the first chunk line until CRLF.
	var bytesRead, hexChunkSize, hexChunkSignature []byte
	bytesRead, cr.err = readChunkLine(cr.reader)
	// Parse s3 specific chunk extension and fetch the values.
	hexChunkSize, hexChunkSignature = parseS3ChunkExtension(bytesRead)

	if cr.err != nil {
		return
	}
	// <hex>;token=value - converts the hex into its uint64 form.
	cr.n, cr.err = parseHexUint(hexChunkSize)
	if cr.err != nil {
		return
	}
	if cr.n == 0 {
		cr.err = io.EOF
	}

	// Save the incoming chunk signature.
	if hexChunkSignature == nil {
		// We are using unsigned streaming upload.
		cr.chunkSignature = ""
	} else {
		cr.chunkSignature = string(hexChunkSignature)
	}
}

type chunkState int

const (
	readChunkHeader chunkState = iota
	readChunkTrailer
	readChunk
	readTrailerChunk
	verifyChunk
	verifyChecksum
	eofChunk
)

func (cs chunkState) String() string {
	stateString := ""
	switch cs {
	case readChunkHeader:
		stateString = "readChunkHeader"
	case readChunkTrailer:
		stateString = "readChunkTrailer"
	case readChunk:
		stateString = "readChunk"
	case readTrailerChunk:
		stateString = "readTrailerChunk"
	case verifyChunk:
		stateString = "verifyChunk"
	case verifyChecksum:
		stateString = "verifyChecksum"
	case eofChunk:
		stateString = "eofChunk"

	}
	return stateString
}

func (cr *s3ChunkedReader) Close() (err error) {
	return nil
}

// Read - implements `io.Reader`, which transparently decodes
// the incoming AWS Signature V4 streaming signature.
func (cr *s3ChunkedReader) Read(buf []byte) (n int, err error) {
	for {
		switch cr.state {
		case readChunkHeader:
			cr.readS3ChunkHeader()
			// If we're at the end of a chunk.
			if cr.n == 0 && cr.err == io.EOF {
				cr.state = readChunkTrailer
				cr.lastChunk = true
				continue
			}
			if cr.err != nil {
				return 0, cr.err
			}
			cr.state = readChunk
		case readChunkTrailer:
			err = peekCRLF(cr.reader)
			isTrailingChunk := cr.n == 0 && cr.lastChunk

			if !isTrailingChunk {
				// If we're not in the trailing chunk, we should consume the bytes no matter what.
				// The error returned by peekCRLF is the same as the one by readCRLF.
				readCRLF(cr.reader)
				cr.err = err
			} else if err != nil && err != errMalformedEncoding {
				cr.err = err
				return 0, errMalformedEncoding
			} else { // equivalent to isTrailingChunk && err == errMalformedEncoding
				// FIXME: 	The "right" structure of the last chunk as provided by the examples in the
				// 			AWS documentation is "0\r\n\r\n" instead of "0\r\n", but some s3 clients when calling with
				// 			streaming-unsigned-payload-trailer omit the last CRLF. To avoid returning an error that, we need to accept both.
				// We arrive here when we're at the end of the 0-byte chunk, depending on the client implementation
				// the client may or may not send the optional CRLF after the 0-byte chunk.
				// If the client sends the optional CRLF, we should consume it.
				if err == nil {
					readCRLF(cr.reader)
				}
			}

			// If we're using unsigned streaming upload, there is no signature to verify at each chunk.
			if cr.chunkSignature != "" {
				cr.state = verifyChunk
			} else if cr.lastChunk {
				cr.state = readTrailerChunk
			} else {
				cr.state = readChunkHeader
			}

		case readTrailerChunk:
			// When using unsigned upload, this would be the raw contents of  the trailer chunk:
			//
			// x-amz-checksum-crc32:YABb/g==\n\r\n\r\n      // Trailer chunk (note optional \n character)
			// \r\n                                         // CRLF
			//
			// When using signed upload with an additional checksum algorithm, this would be the raw contents of the trailer chunk:
			//
			// x-amz-checksum-crc32:YABb/g==\n\r\n            // Trailer chunk (note optional \n character)
			// trailer-signature\r\n
			// \r\n                                           // CRLF
			//
			// This implementation currently only supports the first case.
			// TODO: Implement the second case (signed upload with additional checksum computation for each chunk)

			extractedCheckSumAlgorithm, extractedChecksum := parseChunkChecksum(cr.reader)

			if extractedCheckSumAlgorithm.String() != cr.checkSumAlgorithm {
				errorMessage := fmt.Sprintf("checksum algorithm in trailer '%s' does not match the one advertised in the header '%s'", extractedCheckSumAlgorithm.String(), cr.checkSumAlgorithm)
				glog.V(3).Info(errorMessage)
				cr.err = errors.New(errorMessage)
				return 0, cr.err
			}

			computedChecksum := cr.checkSumWriter.Sum(nil)
			base64Checksum := base64.StdEncoding.EncodeToString(computedChecksum)
			if string(extractedChecksum) != base64Checksum {
				// TODO: Return BadDigest
				glog.V(3).Infof("payload checksum '%s' does not match provided checksum '%s'", base64Checksum, string(extractedChecksum))
				cr.err = errors.New("payload checksum does not match")
				return 0, cr.err
			}

			// TODO: Extract signature from trailer chunk and verify it.
			// For now, we just read the trailer chunk and discard it.

			// Reading remaining CRLF.
			for i := 0; i < 2; i++ {
				cr.err = readCRLF(cr.reader)
			}

			cr.state = eofChunk

		case readChunk:
			// There is no more space left in the request buffer.
			if len(buf) == 0 {
				return n, nil
			}
			rbuf := buf
			// The request buffer is larger than the current chunk size.
			// Read only the current chunk from the underlying reader.
			if uint64(len(rbuf)) > cr.n {
				rbuf = rbuf[:cr.n]
			}
			var n0 int
			n0, cr.err = cr.reader.Read(rbuf)
			if cr.err != nil {
				// We have lesser than chunk size advertised in chunkHeader, this is 'unexpected'.
				if cr.err == io.EOF {
					cr.err = io.ErrUnexpectedEOF
				}
				return 0, cr.err
			}

			// Calculate sha256.
			cr.chunkSHA256Writer.Write(rbuf[:n0])

			// Compute checksum
			if cr.checkSumWriter != nil {
				cr.checkSumWriter.Write(rbuf[:n0])
			}

			// Update the bytes read into request buffer so far.
			n += n0
			buf = buf[n0:]
			// Update bytes to be read of the current chunk before verifying chunk's signature.
			cr.n -= uint64(n0)

			// If we're at the end of a chunk.
			if cr.n == 0 {
				cr.state = readChunkTrailer
				continue
			}
		case verifyChunk:
			// Calculate the hashed chunk.
			hashedChunk := hex.EncodeToString(cr.chunkSHA256Writer.Sum(nil))
			// Calculate the chunk signature.
			newSignature := cr.getChunkSignature(hashedChunk)
			if !compareSignatureV4(cr.chunkSignature, newSignature) {
				// Chunk signature doesn't match we return signature does not match.
				cr.err = errors.New("chunk signature does not match")
				return 0, cr.err
			}
			// Newly calculated signature becomes the seed for the next chunk
			// this follows the chaining.
			cr.seedSignature = newSignature
			cr.chunkSHA256Writer.Reset()
			if cr.lastChunk {
				cr.state = eofChunk
			} else {
				cr.state = readChunkHeader
			}
		case eofChunk:
			return n, io.EOF
		}
	}
}

// getChunkSignature - get chunk signature.
func (cr *s3ChunkedReader) getChunkSignature(hashedChunk string) string {
	// Calculate string to sign.
	stringToSign := signV4ChunkedAlgorithm + "\n" +
		cr.seedDate.Format(iso8601Format) + "\n" +
		getScope(cr.seedDate, cr.region) + "\n" +
		cr.seedSignature + "\n" +
		emptySHA256 + "\n" +
		hashedChunk

	// Calculate signature.
	return cr.iam.getSignature(
		cr.cred.SecretKey,
		cr.seedDate,
		cr.region,
		"s3",
		stringToSign,
	)
}

// readCRLF - check if reader only has '\r\n' CRLF character.
// returns malformed encoding if it doesn't.
func readCRLF(reader *bufio.Reader) error {
	buf := make([]byte, 2)
	_, err := reader.Read(buf)
	if err != nil {
		return err
	}
	return checkCRLF(buf)
}

// peekCRLF - peeks at the next two bytes to check for CRLF without consuming them.
func peekCRLF(reader *bufio.Reader) error {
	peeked, err := reader.Peek(2)
	if err != nil {
		return err
	}
	if err := checkCRLF(peeked); err != nil {
		return err
	}
	return nil
}

// checkCRLF - checks if the buffer contains '\r\n' CRLF character.
func checkCRLF(buf []byte) error {
	if buf[0] != '\r' || buf[1] != '\n' {
		return errMalformedEncoding
	}
	return nil
}

// Read a line of bytes (up to \n) from b.
// Give up if the line exceeds maxLineLength.
// The returned bytes are owned by the bufio.Reader
// so they are only valid until the next bufio read.
func readChunkLine(b *bufio.Reader) ([]byte, error) {
	buf, err := b.ReadSlice('\n')
	if err != nil {
		// We always know when EOF is coming.
		// If the caller asked for a line, there should be a line.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		} else if err == bufio.ErrBufferFull {
			err = errLineTooLong
		}
		return nil, err
	}
	if len(buf) >= maxLineLength {
		return nil, errLineTooLong
	}

	return buf, nil
}

// trimTrailingWhitespace - trim trailing white space.
func trimTrailingWhitespace(b []byte) []byte {
	for len(b) > 0 && isASCIISpace(b[len(b)-1]) {
		b = b[:len(b)-1]
	}
	return b
}

// isASCIISpace - is ascii space?
func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// Constant s3 chunk encoding signature.
const s3ChunkSignatureStr = ";chunk-signature="

// parseS3ChunkExtension removes any s3 specific chunk-extension from buf.
// For example,
//
//	"10000;chunk-signature=..." => "10000", "chunk-signature=..."
func parseS3ChunkExtension(buf []byte) ([]byte, []byte) {
	buf = trimTrailingWhitespace(buf)
	semi := bytes.Index(buf, []byte(s3ChunkSignatureStr))
	// Chunk signature not found, return the whole buffer.
	// This means we're using unsigned streaming upload.
	if semi == -1 {
		return buf, nil
	}
	return buf[:semi], parseChunkSignature(buf[semi:])
}

func parseChunkChecksum(b *bufio.Reader) (ChecksumAlgorithm, []byte) {
	// When using unsigned upload, this would be the raw contents of  the trailer chunk:
	//
	// x-amz-checksum-crc32:YABb/g==\n\r\n\r\n      // Trailer chunk (note optional \n character)
	// \r\n                                         // CRLF
	//
	// When using signed upload with an additional checksum algorithm, this would be the raw contents of the trailer chunk:
	//
	// x-amz-checksum-crc32:YABb/g==\n\r\n            // Trailer chunk (note optional \n character)
	// trailer-signature\r\n
	// \r\n                                           // CRLF
	//

	// x-amz-checksum-crc32:YABb/g==\n
	bytesRead, err := readChunkLine(b)
	if err != nil {
		return ChecksumAlgorithmNone, nil
	}

	// Split on ':'
	parts := bytes.SplitN(bytesRead, []byte(":"), 2)
	checksumKey := string(parts[0])
	checksumValue := parts[1]

	// Discard all trailing whitespace characters
	checksumValue = trimTrailingWhitespace(checksumValue)

	// If the checksum key is not a supported checksum algorithm, return an error.
	// TODO: Bubble that error up to the caller
	extractedAlgorithm, err := extractChecksumAlgorithm(checksumKey)
	if err != nil {
		return ChecksumAlgorithmNone, nil
	}

	return extractedAlgorithm, checksumValue
}

// parseChunkSignature - parse chunk signature.
func parseChunkSignature(chunk []byte) []byte {
	chunkSplits := bytes.SplitN(chunk, []byte(s3ChunkSignatureStr), 2)
	return chunkSplits[1]
}

// parse hex to uint64.
func parseHexUint(v []byte) (n uint64, err error) {
	for i, b := range v {
		switch {
		case '0' <= b && b <= '9':
			b = b - '0'
		case 'a' <= b && b <= 'f':
			b = b - 'a' + 10
		case 'A' <= b && b <= 'F':
			b = b - 'A' + 10
		default:
			return 0, errors.New("invalid byte in chunk length")
		}
		if i == 16 {
			return 0, errors.New("http chunk length too large")
		}
		n <<= 4
		n |= uint64(b)
	}
	return
}

type ChecksumAlgorithm int

const (
	ChecksumAlgorithmNone ChecksumAlgorithm = iota
	ChecksumAlgorithmCRC32
	ChecksumAlgorithmCRC32C
	ChecksumAlgorithmCRC64NVMe
	ChecksumAlgorithmSHA1
	ChecksumAlgorithmSHA256
)

func (ca ChecksumAlgorithm) String() string {
	switch ca {
	case ChecksumAlgorithmCRC32:
		return "CRC32"
	case ChecksumAlgorithmCRC32C:
		return "CRC32C"
	case ChecksumAlgorithmCRC64NVMe:
		return "CRC64NVMe"
	case ChecksumAlgorithmSHA1:
		return "SHA1"
	case ChecksumAlgorithmSHA256:
		return "SHA256"
	case ChecksumAlgorithmNone:
		return ""
	}
	return ""
}

// getCheckSumWriter - get checksum writer.
func getCheckSumWriter(checksumAlgorithm ChecksumAlgorithm) hash.Hash {
	switch checksumAlgorithm {
	case ChecksumAlgorithmCRC32:
		return crc32.NewIEEE()
	case ChecksumAlgorithmCRC32C:
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case ChecksumAlgorithmCRC64NVMe:
		return crc64nvme.New()
	case ChecksumAlgorithmSHA1:
		return sha1.New()
	case ChecksumAlgorithmSHA256:
		return sha256.New()
	}
	return nil
}
