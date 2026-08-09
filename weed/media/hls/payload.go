package hls

import (
	"fmt"
	"io"
	"math"
)

// WalkSegmentChunks reads each segment from reader and splits it into storage
// chunks no larger than maxChunkSize, invoking fn once per chunk with the
// chunk's absolute byte offset in the media file and a fresh slice owned by the
// caller. A chunk never spans a segment boundary, so every segment maps to a
// contiguous run of whole chunks and a later segment read fetches only those
// chunks instead of slicing a larger object; the trailing chunk of a segment
// holds only the remainder so no padding is stored or read. maxChunkSize <= 0
// stores each segment as a single chunk.
//
// The function rejects truncated input and trailing bytes, which prevents
// committing an entry whose logical file differs from the playlist used to
// segment it.
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

	// io.Reader is allowed to return (0, nil), especially for streaming readers.
	// ReadFull keeps reading until it sees one trailing byte or a definitive EOF.
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
