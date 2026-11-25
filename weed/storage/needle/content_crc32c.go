package needle

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// ContentCrc32cHeader is the request header an uploader sets to the CRC32C
// (Castagnoli) of a chunk's file data, so the volume server and its replicas
// refuse bytes that changed in transit. It covers the bytes Content-MD5 is
// checked against: the file data after undoing Content-Encoding: gzip. The
// value is the std-base64 of the big-endian checksum, the encoding of S3's
// x-amz-checksum-crc32c. The name stays outside PairNamePrefix, so it is never
// stored as a needle pair.
const ContentCrc32cHeader = "X-Seaweed-Content-Crc32c"

// EncodeContentCrc32c formats a checksum as a ContentCrc32cHeader value.
func EncodeContentCrc32c(c CRC) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(c))
	return base64.StdEncoding.EncodeToString(b[:])
}

// ContentCrc32c is the ContentCrc32cHeader value for data.
func ContentCrc32c(data []byte) string {
	return EncodeContentCrc32c(NewCRC(data))
}

// ParseContentCrc32c reads a ContentCrc32cHeader value. Strict decoding takes
// only the canonical form, as the Rust volume server does.
func ParseContentCrc32c(value string) (CRC, error) {
	b, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(b) != 4 {
		return 0, fmt.Errorf("%s [%s] is not the base64 of a 4-byte crc32c", ContentCrc32cHeader, value)
	}
	return CRC(binary.BigEndian.Uint32(b)), nil
}

// checkContentCrc32c refuses file data whose checksum differs from the one
// the uploader sent, in the style of the Content-MD5 check.
func checkContentCrc32c(expected string, actual CRC, size int) error {
	want, err := ParseContentCrc32c(expected)
	if err != nil {
		return err
	}
	if want != actual {
		return fmt.Errorf("%s did not match crc32c of file data expected [%s] received [%s] size %d", ContentCrc32cHeader, expected, EncodeContentCrc32c(actual), size)
	}
	return nil
}
