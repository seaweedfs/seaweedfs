package protocol

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
)

func TestMultiBatchFetcher_compressData(t *testing.T) {
	fetcher := &MultiBatchFetcher{
		handler: &Handler{},
	}

	testData := []byte("This is test data for compression. It should compress well because it has repeated patterns. " +
		"This is test data for compression. It should compress well because it has repeated patterns.")

	tests := []struct {
		name     string
		codec    compression.CompressionCodec
		wantErr  bool
		validate func(t *testing.T, original, compressed []byte)
	}{
		{
			name:    "no compression",
			codec:   compression.None,
			wantErr: false,
			validate: func(t *testing.T, original, compressed []byte) {
				if !bytes.Equal(original, compressed) {
					t.Error("No compression should return original data unchanged")
				}
			},
		},
		{
			name:    "gzip compression",
			codec:   compression.Gzip,
			wantErr: false,
			validate: func(t *testing.T, original, compressed []byte) {
				// Compressed data should be smaller for repetitive data
				if len(compressed) >= len(original) {
					t.Errorf("GZIP compression should reduce size: original=%d, compressed=%d", 
						len(original), len(compressed))
				}

				// Verify we can decompress it back
				reader, err := gzip.NewReader(bytes.NewReader(compressed))
				if err != nil {
					t.Fatalf("Failed to create gzip reader: %v", err)
				}
				defer reader.Close()

				var decompressed bytes.Buffer
				if _, err := decompressed.ReadFrom(reader); err != nil {
					t.Fatalf("Failed to decompress: %v", err)
				}

				if !bytes.Equal(original, decompressed.Bytes()) {
					t.Error("Decompressed data doesn't match original")
				}
			},
		},
		{
			name:    "unsupported compression",
			codec:   compression.CompressionCodec(99), // Invalid codec
			wantErr: true,
			validate: func(t *testing.T, original, compressed []byte) {
				// Should be nil on error
				if compressed != nil {
					t.Error("Expected nil result for unsupported compression")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fetcher.compressData(testData, tt.codec)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}

			if tt.validate != nil {
				tt.validate(t, testData, result)
			}
		})
	}
}

func TestGzipCompressionRatio(t *testing.T) {
	fetcher := &MultiBatchFetcher{
		handler: &Handler{},
	}

	// Test with different types of data
	tests := []struct {
		name         string
		data         []byte
		expectRatio  float64 // Expected compression ratio (compressed/original)
	}{
		{
			name:        "highly repetitive data",
			data:        bytes.Repeat([]byte("AAAA"), 1000),
			expectRatio: 0.02, // Should compress very well (allowing for gzip overhead)
		},
		{
			name:        "json-like data",
			data:        []byte(`{"key":"value","array":[1,2,3,4,5],"nested":{"inner":"data"}}`),
			expectRatio: 0.8, // Moderate compression
		},
		{
			name:        "small data",
			data:        []byte("small"),
			expectRatio: 2.0, // May actually expand due to gzip overhead
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := fetcher.compressData(tt.data, compression.Gzip)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			ratio := float64(len(compressed)) / float64(len(tt.data))
			t.Logf("Compression ratio: %.3f (original: %d bytes, compressed: %d bytes)", 
				ratio, len(tt.data), len(compressed))

			// For highly repetitive data, we expect good compression
			if tt.name == "highly repetitive data" && ratio > tt.expectRatio {
				t.Errorf("Expected compression ratio < %.2f, got %.3f", tt.expectRatio, ratio)
			}
		})
	}
}
