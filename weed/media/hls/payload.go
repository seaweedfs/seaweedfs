package hls

import (
	"fmt"
	"io"
	"math"
)

// WalkSegmentChunks reads media segments sequentially and splits each segment
// into chunks no larger than maxChunkSize. Chunks never cross segment
// boundaries. A non-positive maxChunkSize keeps each segment in one chunk.
// Truncated input and trailing media data are rejected.
func WalkSegmentChunks(reader io.Reader, metadata *Metadata, maxChunkSize int64, fn func(segmentIndex int, chunkOffset int64, data []byte) error) error {
	if err := Validate(metadata, -1, 0); err != nil {
		return err
	}
	for i, segment := range metadata.Segments {
		offset := segment.Offset
		remaining := segment.Size
		for remaining > 0 {
			chunkSize := remaining
			if maxChunkSize > 0 && chunkSize > maxChunkSize {
				chunkSize = maxChunkSize
			}
			if chunkSize > int64(math.MaxInt) {
				return fmt.Errorf("segment %d chunk at offset %d is too large for this platform: %d", i, offset, chunkSize)
			}
			data := make([]byte, chunkSize)
			if _, err := io.ReadFull(reader, data); err != nil {
				return fmt.Errorf("read segment %d chunk at offset %d (%d bytes): %w", i, offset, chunkSize, err)
			}
			if err := fn(i, offset, data); err != nil {
				return fmt.Errorf("process segment %d chunk at offset %d: %w", i, offset, err)
			}
			offset += chunkSize
			remaining -= chunkSize
		}
	}

	// ReadFull tolerates readers that transiently return (0, nil) before EOF.
	var extra [1]byte
	n, err := io.ReadFull(reader, extra[:])
	if n != 0 {
		return fmt.Errorf("media payload has trailing bytes after the last segment")
	}
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check media payload end: %w", err)
	}
	return fmt.Errorf("media payload has trailing bytes after the last segment")
}

// TotalSize returns the total media size described by metadata.
func TotalSize(metadata *Metadata) int64 {
	var total int64
	if metadata == nil {
		return 0
	}
	for _, segment := range metadata.Segments {
		total += segment.Size
	}
	return total
}
