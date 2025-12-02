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
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"github.com/dustin/go-humanize"
	"github.com/minio/crc64nvme"
)

// calculateSeedSignature - Calculate seed signature in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
//
// returns signature, error otherwise if the signature mismatches or any other
// error while parsing and validating.
func (iam *IdentityAccessManagement) calculateSeedSignature(r *http.Request) (cred *Credential, signature string, region string, service string, date time.Time, errCode s3err.ErrorCode) {
	_, credential, calculatedSignature, authInfo, errCode := iam.verifyV4Signature(r, true)
	if errCode != s3err.ErrNone {
		return nil, "", "", "", time.Time{}, errCode
	}

	// This check ensures we only proceed for streaming uploads.
	switch authInfo.HashedPayload {
	case streamingContentSHA256:
		glog.V(3).Infof("streaming content sha256")
	case streamingUnsignedPayload:
		glog.V(3).Infof("streaming unsigned payload")
	default:
		return nil, "", "", "", time.Time{}, s3err.ErrContentSHA256Mismatch
	}

	return credential, calculatedSignature, authInfo.Region, authInfo.Service, authInfo.Date, s3err.ErrNone
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

	var credential *Credential
	var seedSignature, region, service string
	var seedDate time.Time
	var errCode s3err.ErrorCode

	switch contentSha256Header {
	// Payload for STREAMING signature should be 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'
	case streamingContentSHA256:
		glog.V(3).Infof("streaming content sha256")
		credential, seedSignature, region, service, seedDate, errCode = iam.calculateSeedSignature(req)
		if errCode != s3err.ErrNone {
			return nil, errCode
		}
	case streamingUnsignedPayload:
		glog.V(3).Infof("streaming unsigned payload")
		if authorizationHeader != "" {
			// We do not need to pass the seed signature to the Reader as each chunk is not signed,
			// but we do compute it to verify the caller has the correct permissions.
			_, _, _, _, _, errCode = iam.calculateSeedSignature(req)
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
	hasTrailer := amzTrailerHeader != ""

	return &s3ChunkedReader{
		cred:              credential,
		reader:            bufio.NewReader(req.Body),
		seedSignature:     seedSignature,
		seedDate:          seedDate,
		region:            region,
		service:           service,
		chunkSHA256Writer: sha256.New(),
		checkSumAlgorithm: checksumAlgorithm.String(),
		checkSumWriter:    checkSumWriter,
		state:             readChunkHeader,
		iam:               iam,
		hasTrailer:        hasTrailer,
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
	service           string // Service from credential scope (e.g., "s3", "iam")
	state             chunkState
	lastChunk         bool
	chunkSignature    string // Empty string if unsigned streaming upload.
	checkSumAlgorithm string // Empty string if no checksum algorithm is specified.
	checkSumWriter    hash.Hash
	chunkSHA256Writer hash.Hash // Calculates sha256 of chunk data.
	n                 uint64    // Unread bytes in chunk
	err               error
	iam               *IdentityAccessManagement
	hasTrailer        bool
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
			if cr.lastChunk && cr.hasTrailer {
				cr.state = readTrailerChunk
			} else if cr.chunkSignature != "" {
				cr.state = verifyChunk
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

			extractedCheckSumAlgorithm, extractedChecksum, err := parseChunkChecksum(cr.reader)
			if err != nil {
				cr.err = err
				return 0, err
			}

			if extractedCheckSumAlgorithm.String() != cr.checkSumAlgorithm {
				errorMessage := fmt.Sprintf("checksum algorithm in trailer '%s' does not match the one advertised in the header '%s'", extractedCheckSumAlgorithm.String(), cr.checkSumAlgorithm)
				glog.V(3).Info(errorMessage)
				cr.err = errors.New(s3err.ErrMsgChecksumAlgorithmMismatch)
				return 0, cr.err
			}

			// Validate checksum for data integrity (required for both signed and unsigned streaming with trailers)
			computedChecksum := cr.checkSumWriter.Sum(nil)
			base64Checksum := base64.StdEncoding.EncodeToString(computedChecksum)
			if string(extractedChecksum) != base64Checksum {
				glog.V(3).Infof("payload checksum '%s' does not match provided checksum '%s'", base64Checksum, string(extractedChecksum))
				cr.err = errors.New(s3err.ErrMsgPayloadChecksumMismatch)
				return 0, cr.err
			}

			// TODO: Extract signature from trailer chunk and verify it.
			// For now, we just read the trailer chunk and discard it.

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
			}
		case verifyChunk:
			// Check if we have credentials for signature verification
			// This handles the case where we have unsigned streaming (no cred) but chunks contain signatures
			//
			// BUG FIX for GitHub issue #6847:
			// Some AWS SDK versions (Java 3.7.412+, .NET 4.0.0-preview.6+) send mixed format:
			// - HTTP headers indicate unsigned streaming (STREAMING-UNSIGNED-PAYLOAD-TRAILER)
			// - But chunk data contains chunk-signature headers (normally only for signed streaming)
			// This causes a nil pointer dereference when trying to verify signatures without credentials
			if cr.cred != nil {
				// Normal signed streaming - verify the chunk signature
				// Calculate the hashed chunk.
				hashedChunk := hex.EncodeToString(cr.chunkSHA256Writer.Sum(nil))
				// Calculate the chunk signature.
				newSignature := cr.getChunkSignature(hashedChunk)
				if !compareSignatureV4(cr.chunkSignature, newSignature) {
					// Chunk signature doesn't match we return signature does not match.
					cr.err = errors.New(s3err.ErrMsgChunkSignatureMismatch)
					return 0, cr.err
				}
				// Newly calculated signature becomes the seed for the next chunk
				// this follows the chaining.
				cr.seedSignature = newSignature
			} else {
				// For unsigned streaming, we should not verify chunk signatures even if they are present
				// This fixes the bug where AWS SDKs send chunk signatures with unsigned streaming headers
				glog.V(3).Infof("Skipping chunk signature verification for unsigned streaming")
			}

			// Common cleanup and state transition for both signed and unsigned streaming
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
	stringToSign := signV4Algorithm + "-PAYLOAD" + "\n" +
		cr.seedDate.Format(iso8601Format) + "\n" +
		getScope(cr.seedDate, cr.region, cr.service) + "\n" +
		cr.seedSignature + "\n" +
		emptySHA256 + "\n" +
		hashedChunk

	// Get hmac signing key.
	signingKey := getSigningKey(cr.cred.SecretKey, cr.seedDate.Format(yyyymmdd), cr.region, cr.service)

	// Calculate and return signature.
	return getSignature(signingKey, stringToSign)
}

func readCRLF(reader *bufio.Reader) error {
	buf := make([]byte, 2)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return err
	}
	return checkCRLF(buf)
}

func peekCRLF(reader *bufio.Reader) error {
	buf, err := reader.Peek(2)
	if err != nil {
		return err
	}
	if err := checkCRLF(buf); err != nil {
		return err
	}
	return nil
}

func checkCRLF(buf []byte) error {
	if len(buf) != 2 || buf[0] != '\r' || buf[1] != '\n' {
		return errMalformedEncoding
	}
	return nil
}

func readChunkLine(b *bufio.Reader) ([]byte, error) {
	buf, err := b.ReadSlice('\n')
	if err != nil {
		// We always know when EOF is coming.
		// If the caller asked for a line, there should be a line.
		switch err {
		case io.EOF:
			err = io.ErrUnexpectedEOF
		case bufio.ErrBufferFull:
			err = errLineTooLong
		}
		return nil, err
	}
	if len(buf) >= maxLineLength {
		return nil, errLineTooLong
	}
	return trimTrailingWhitespace(buf), nil
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

func parseChunkChecksum(b *bufio.Reader) (ChecksumAlgorithm, []byte, error) {
	// Read trailer lines until empty line
	var checksumAlgorithm ChecksumAlgorithm
	var checksum []byte

	for {
		bytesRead, err := readChunkLine(b)
		if err != nil {
			return ChecksumAlgorithmNone, nil, err
		}

		line := trimTrailingWhitespace(bytesRead)
		if len(line) == 0 {
			break
		}

		parts := bytes.SplitN(line, []byte(":"), 2)
		if len(parts) == 2 {
			key := string(parts[0])
			value := trimTrailingWhitespace(parts[1])
			if alg, err := extractChecksumAlgorithm(key); err == nil {
				checksumAlgorithm = alg
				checksum = value
			}
			// Ignore other trailer headers like x-amz-trailer-signature
		}
	}

	return checksumAlgorithm, checksum, nil
}

func parseChunkSignature(chunk []byte) []byte {
	chunkSplits := bytes.SplitN(chunk, []byte("="), 2)
	return chunkSplits[1] // Keep only the signature.
}

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

// Checksum Algorithm represents the various checksum algorithms supported.
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
	case ChecksumAlgorithmNone:
		return ""
	case ChecksumAlgorithmCRC32:
		return "x-amz-checksum-crc32"
	case ChecksumAlgorithmCRC32C:
		return "x-amz-checksum-crc32c"
	case ChecksumAlgorithmCRC64NVMe:
		return "x-amz-checksum-crc64nvme"
	case ChecksumAlgorithmSHA1:
		return "x-amz-checksum-sha1"
	case ChecksumAlgorithmSHA256:
		return "x-amz-checksum-sha256"
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
