package s3api

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"hash/crc32"
	"io"
	"testing"
)

// segmentedReader returns one segment per Read call.
type segmentedReader struct{ segments [][]byte }

func (s *segmentedReader) Read(p []byte) (int, error) {
	if len(s.segments) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.segments[0])
	s.segments[0] = s.segments[0][n:]
	if len(s.segments[0]) == 0 {
		s.segments = s.segments[1:]
	}
	return n, nil
}

func TestTrailerChecksumSurvivesSplitTrailerLines(t *testing.T) {
	payload := []byte("hello, trailer\n")
	crcWriter := crc32.NewIEEE()
	crcWriter.Write(payload)
	checksum := base64.StdEncoding.EncodeToString(crcWriter.Sum(nil))
	sig := "0000000000000000000000000000000000000000000000000000000000000000"

	segments := [][]byte{
		[]byte("f;chunk-signature=" + sig + "\r\n"),
		append(payload, "\r\n"...),
		[]byte("0;chunk-signature=" + sig + "\r\n"),
		[]byte("x-amz-checksum-crc32:" + checksum),
		[]byte("\r\n"),
		[]byte("x-amz-trailer-signature:" + sig + "\r\n\r\n"),
	}

	cr := &s3ChunkedReader{
		reader:            bufio.NewReader(&segmentedReader{segments: segments}),
		chunkSHA256Writer: sha256.New(),
		checkSumAlgorithm: ChecksumAlgorithmCRC32.String(),
		checkSumWriter:    getCheckSumWriter(ChecksumAlgorithmCRC32),
		state:             readChunkHeader,
		hasTrailer:        true,
	}

	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}
