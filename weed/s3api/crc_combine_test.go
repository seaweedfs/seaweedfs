package s3api

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/minio/crc64nvme"
)

// Tests that combining per-block CRC-64/NVME values reproduces the CRC of the concatenated data.
func TestCombineCRC64NVME(t *testing.T) {
	params := crcCombineParams[ChecksumAlgorithmCRC64NVMe]

	blocks := [][]byte{
		[]byte("abc"),
		[]byte("def"),
		[]byte("hello world, this is a longer block of data to combine"),
		bytes.Repeat([]byte{0x00}, 5*1024*1024),
		bytes.Repeat([]byte{0xAB, 0xCD}, 1024),
		[]byte("z"),
	}

	var combined uint64
	var concatenated []byte
	for i, block := range blocks {
		blockCRC := crc64nvme.Checksum(block)

		if i == 0 {
			combined = blockCRC
		} else {
			combined = combineCRC(combined, blockCRC, uint64(len(block)), params)
		}
		concatenated = append(concatenated, block...)

		want := crc64nvme.Checksum(concatenated)
		if combined != want {
			t.Fatalf("after block %d: combined = %016x, want %016x", i, combined, want)
		}
	}
}

// Tests that combining per-block CRC-32 values reproduces the CRC of the concatenated data.
func TestCombineCRC32(t *testing.T) {
	params := crcCombineParams[ChecksumAlgorithmCRC32]

	blocks := [][]byte{
		[]byte("abc"),
		[]byte("def"),
		[]byte("hello world, this is a longer block of data to combine"),
		bytes.Repeat([]byte{0x00}, 5*1024*1024),
		bytes.Repeat([]byte{0xAB, 0xCD}, 1024),
		[]byte("z"),
	}

	var combined uint64
	var concatenated []byte
	for i, block := range blocks {
		blockCRC := uint64(crc32.Checksum(block, crc32.MakeTable(crc32.IEEE)))

		if i == 0 {
			combined = blockCRC
		} else {
			combined = combineCRC(combined, blockCRC, uint64(len(block)), params)
		}
		concatenated = append(concatenated, block...)

		want := uint64(crc32.Checksum(concatenated, crc32.MakeTable(crc32.IEEE)))
		if combined != want {
			t.Fatalf("after block %d: combined = %016x, want %016x", i, combined, want)
		}
	}
}

func TestCombineCRC64NVMEEmptySecond(t *testing.T) {
	params := crcCombineParams[ChecksumAlgorithmCRC64NVMe]
	crc1 := crc64nvme.Checksum([]byte("abc"))

	if got := combineCRC(crc1, crc64nvme.Checksum(nil), 0, params); got != crc1 {
		t.Fatalf("combining with empty second block changed crc: got %016x, want %016x", got, crc1)
	}
}

func TestCombineCRC32EmptySecond(t *testing.T) {
	params := crcCombineParams[ChecksumAlgorithmCRC32]
	crc1 := uint64(crc32.Checksum([]byte("abc"), crc32.MakeTable(crc32.IEEE)))

	if got := combineCRC(crc1, uint64(crc32.Checksum(nil, crc32.MakeTable(crc32.IEEE))), 0, params); got != crc1 {
		t.Fatalf("combining with empty second block changed crc: got %016x, want %016x", got, crc1)
	}
}
