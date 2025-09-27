package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// nopCloser wraps an io.Reader to provide a no-op Close method
type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

// CompressionCodec represents the compression codec used in Kafka record batches
type CompressionCodec int8

const (
	None   CompressionCodec = 0
	Gzip   CompressionCodec = 1
	Snappy CompressionCodec = 2
	Lz4    CompressionCodec = 3
	Zstd   CompressionCodec = 4
)

// String returns the string representation of the compression codec
func (c CompressionCodec) String() string {
	switch c {
	case None:
		return "none"
	case Gzip:
		return "gzip"
	case Snappy:
		return "snappy"
	case Lz4:
		return "lz4"
	case Zstd:
		return "zstd"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

// IsValid returns true if the compression codec is valid
func (c CompressionCodec) IsValid() bool {
	return c >= None && c <= Zstd
}

// ExtractCompressionCodec extracts the compression codec from record batch attributes
func ExtractCompressionCodec(attributes int16) CompressionCodec {
	return CompressionCodec(attributes & 0x07) // Lower 3 bits
}

// SetCompressionCodec sets the compression codec in record batch attributes
func SetCompressionCodec(attributes int16, codec CompressionCodec) int16 {
	return (attributes &^ 0x07) | int16(codec)
}

// Compress compresses data using the specified codec
func Compress(codec CompressionCodec, data []byte) ([]byte, error) {
	if codec == None {
		return data, nil
	}

	var buf bytes.Buffer
	var writer io.WriteCloser
	var err error

	switch codec {
	case Gzip:
		writer = gzip.NewWriter(&buf)
	case Snappy:
		// Snappy doesn't have a streaming writer, so we compress directly
		compressed := snappy.Encode(nil, data)
		if compressed == nil {
			compressed = []byte{}
		}
		return compressed, nil
	case Lz4:
		writer = lz4.NewWriter(&buf)
	case Zstd:
		writer, err = zstd.NewWriter(&buf)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd writer: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression codec: %s", codec)
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write compressed data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close compressor: %w", err)
	}

	return buf.Bytes(), nil
}

// Decompress decompresses data using the specified codec
func Decompress(codec CompressionCodec, data []byte) ([]byte, error) {
	if codec == None {
		return data, nil
	}

	var reader io.ReadCloser
	var err error

	buf := bytes.NewReader(data)

	switch codec {
	case Gzip:
		reader, err = gzip.NewReader(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
	case Snappy:
		// Snappy doesn't have a streaming reader, so we decompress directly
		decompressed, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress snappy data: %w", err)
		}
		if decompressed == nil {
			decompressed = []byte{}
		}
		return decompressed, nil
	case Lz4:
		lz4Reader := lz4.NewReader(buf)
		// lz4.Reader doesn't implement Close, so we wrap it
		reader = &nopCloser{Reader: lz4Reader}
	case Zstd:
		zstdReader, err := zstd.NewReader(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer zstdReader.Close()

		var result bytes.Buffer
		if _, err := io.Copy(&result, zstdReader); err != nil {
			return nil, fmt.Errorf("failed to decompress zstd data: %w", err)
		}
		decompressed := result.Bytes()
		if decompressed == nil {
			decompressed = []byte{}
		}
		return decompressed, nil
	default:
		return nil, fmt.Errorf("unsupported compression codec: %s", codec)
	}

	defer reader.Close()

	var result bytes.Buffer
	if _, err := io.Copy(&result, reader); err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	decompressed := result.Bytes()
	if decompressed == nil {
		decompressed = []byte{}
	}
	return decompressed, nil
}

// CompressRecordBatch compresses the records portion of a Kafka record batch
// This function compresses only the records data, not the entire batch header
func CompressRecordBatch(codec CompressionCodec, recordsData []byte) ([]byte, int16, error) {
	if codec == None {
		return recordsData, 0, nil
	}

	compressed, err := Compress(codec, recordsData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to compress record batch: %w", err)
	}

	attributes := int16(codec)
	return compressed, attributes, nil
}

// DecompressRecordBatch decompresses the records portion of a Kafka record batch
func DecompressRecordBatch(attributes int16, compressedData []byte) ([]byte, error) {
	codec := ExtractCompressionCodec(attributes)

	if codec == None {
		return compressedData, nil
	}

	decompressed, err := Decompress(codec, compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress record batch: %w", err)
	}

	return decompressed, nil
}
