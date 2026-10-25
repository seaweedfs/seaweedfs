package s3tables

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/iceberg-go"
)

// avroFileMagic prefixes every Avro object container file.
var avroFileMagic = []byte{'O', 'b', 'j', 1}

// formatVersionKey is the Avro header entry naming the Iceberg format version.
const formatVersionKey = "format-version"

// ReadManifestList parses an Iceberg manifest list, tolerating writers that
// leave the format version out of the Avro header.
//
// The Iceberg spec pins the header metadata of manifest *files* but says
// nothing about manifest *lists*, so writers disagree: Java and PyIceberg
// record "format-version", DuckDB writes no header metadata at all. iceberg-go
// reads a missing entry as v1, which makes every v2 manifest reachable from
// such a list fail with "manifest file's 'format-version' metadata indicates
// version 2, but entry from manifest list indicates version 1", and silently
// decodes delete manifests as data manifests because v1 has no "content"
// field. Both leave DuckDB-written tables unmaintainable.
//
// When the entry is missing we derive the version from the record schema the
// writer embedded — the fields it carries say which version wrote it — and add
// it to the header before handing the bytes to iceberg-go.
func ReadManifestList(manifestList []byte) ([]iceberg.ManifestFile, error) {
	if patched, ok := withFormatVersion(manifestList); ok {
		manifestList = patched
	}
	return iceberg.ReadManifestList(bytes.NewReader(manifestList))
}

// withFormatVersion returns manifestList with a "format-version" header entry
// spliced in. It reports false when the header already carries one, or when it
// cannot be parsed — the untouched bytes then go to iceberg-go, which reports
// the underlying problem.
func withFormatVersion(manifestList []byte) ([]byte, bool) {
	metadata, terminator, err := readAvroFileMetadata(manifestList)
	if err != nil {
		return nil, false
	}
	if _, ok := metadata[formatVersionKey]; ok {
		return nil, false
	}

	// The metadata map is a sequence of blocks closed by a zero count, so a
	// new single-entry block can be spliced in ahead of that closing count
	// without re-encoding what the writer already wrote.
	block := appendAvroLong(nil, 1)
	block = appendAvroBytes(block, []byte(formatVersionKey))
	block = appendAvroBytes(block, []byte(strconv.Itoa(manifestListFormatVersion(metadata["avro.schema"]))))

	patched := make([]byte, 0, len(manifestList)+len(block))
	patched = append(patched, manifestList[:terminator]...)
	patched = append(patched, block...)
	return append(patched, manifestList[terminator:]...), true
}

// manifestListFormatVersion infers the format version a manifest list was
// written at from the fields of its manifest_file record: v2 added "content",
// "sequence_number" and "min_sequence_number", v3 added "first_row_id".
func manifestListFormatVersion(schema []byte) int {
	var record struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(schema, &record); err != nil {
		return 1
	}
	version := 1
	for _, field := range record.Fields {
		switch field.Name {
		case "content", "sequence_number", "min_sequence_number":
			if version < 2 {
				version = 2
			}
		case "first_row_id":
			version = 3
		}
	}
	return version
}

// readAvroFileMetadata decodes the header metadata map of an Avro object
// container file, returning the map alongside the offset of the zero block
// count that closes it.
func readAvroFileMetadata(data []byte) (map[string][]byte, int, error) {
	if !bytes.HasPrefix(data, avroFileMagic) {
		return nil, 0, errors.New("not an avro object container file")
	}
	metadata := make(map[string][]byte)
	pos := len(avroFileMagic)
	for {
		blockStart := pos
		count, next, err := readAvroLong(data, pos)
		if err != nil {
			return nil, 0, err
		}
		pos = next
		if count == 0 {
			return metadata, blockStart, nil
		}
		if count < 0 {
			// A negative count is followed by the block size in bytes.
			count = -count
			if _, pos, err = readAvroLong(data, pos); err != nil {
				return nil, 0, err
			}
		}
		for i := int64(0); i < count; i++ {
			key, next, err := readAvroBytes(data, pos)
			if err != nil {
				return nil, 0, err
			}
			value, next, err := readAvroBytes(data, next)
			if err != nil {
				return nil, 0, err
			}
			metadata[string(key)] = value
			pos = next
		}
	}
}

// readAvroLong decodes the zig-zag varint at pos, returning it with the offset
// just past it.
func readAvroLong(data []byte, pos int) (int64, int, error) {
	if pos < 0 || pos >= len(data) {
		return 0, 0, fmt.Errorf("avro long at offset %d is out of range", pos)
	}
	value, n := binary.Varint(data[pos:])
	if n <= 0 {
		return 0, 0, fmt.Errorf("avro long at offset %d is truncated", pos)
	}
	return value, pos + n, nil
}

// readAvroBytes decodes the length-prefixed byte sequence at pos, returning it
// with the offset just past it. The result aliases data.
func readAvroBytes(data []byte, pos int) ([]byte, int, error) {
	length, pos, err := readAvroLong(data, pos)
	if err != nil {
		return nil, 0, err
	}
	if length < 0 || int64(len(data)-pos) < length {
		return nil, 0, fmt.Errorf("avro bytes at offset %d are truncated", pos)
	}
	return data[pos : pos+int(length)], pos + int(length), nil
}

func appendAvroLong(dst []byte, value int64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	return append(dst, scratch[:binary.PutVarint(scratch[:], value)]...)
}

func appendAvroBytes(dst []byte, value []byte) []byte {
	return append(appendAvroLong(dst, int64(len(value))), value...)
}
