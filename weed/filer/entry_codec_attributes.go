package filer

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Field numbers from filer.proto. Only the ones this decoder has to recognise
// are named; every other field is handed to the generated unmarshaller as-is.
const (
	entryChunksField     = 3 // Entry.chunks
	fileChunkOffsetField = 2 // FileChunk.offset
	fileChunkSizeField   = 3 // FileChunk.size
)

// attributesScratchPool holds the re-encoded entry, which is the blob minus its
// chunks and so much smaller than what came in.
var attributesScratchPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

type omitChunksKey struct{}

// WithChunksOmitted marks a listing as wanting attributes only. A store may
// then skip building chunk lists its caller would discard; one that ignores the
// hint stays correct, just slower.
func WithChunksOmitted(ctx context.Context) context.Context {
	return context.WithValue(ctx, omitChunksKey{}, true)
}

// ChunksOmitted reports whether the listing wants attributes only.
func ChunksOmitted(ctx context.Context) bool {
	return ctx.Value(omitChunksKey{}) != nil
}

// DecodeListedEntry decodes one listed entry, dropping the chunk list when the
// listing asked for attributes only.
func DecodeListedEntry(ctx context.Context, entry *Entry, blob []byte) error {
	if ChunksOmitted(ctx) {
		return entry.DecodeAttributesOnly(blob)
	}
	return entry.DecodeAttributesAndChunks(blob)
}

// DecodeAttributesOnly fills entry from blob without building its chunk list,
// which is the bulk of the work for anything but a tiny file. Chunks are still
// measured, because an entry whose stored FileSize is zero takes its size from
// them, but no FileChunk is allocated.
//
// entry.Chunks is left nil, so only a caller that reads attributes and nothing
// else may use this.
func (entry *Entry) DecodeAttributesOnly(blob []byte) error {
	scratchPtr := attributesScratchPool.Get().(*[]byte)
	scratch := (*scratchPtr)[:0]
	defer func() {
		*scratchPtr = scratch
		attributesScratchPool.Put(scratchPtr)
	}()

	// The blob is only re-encoded once a chunk is actually found, so an entry
	// with none — every directory, for one — is unmarshalled where it lies.
	var chunkExtent uint64
	var dropping bool
	attributes := blob
	for pos := 0; pos < len(blob); {
		rest := blob[pos:]
		num, typ, tagLen := protowire.ConsumeTag(rest)
		if tagLen < 0 {
			return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, protowire.ParseError(tagLen))
		}
		valLen := protowire.ConsumeFieldValue(num, typ, rest[tagLen:])
		if valLen < 0 {
			return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, protowire.ParseError(valLen))
		}
		if num == entryChunksField && typ == protowire.BytesType {
			chunk, _ := protowire.ConsumeBytes(rest[tagLen:])
			end, err := chunkExtentEnd(chunk)
			if err != nil {
				return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, err)
			}
			if end > chunkExtent {
				chunkExtent = end
			}
			if !dropping {
				dropping = true
				scratch = append(scratch, blob[:pos]...)
			}
		} else if dropping {
			scratch = append(scratch, rest[:tagLen+valLen]...)
		}
		pos += tagLen + valLen
	}
	if dropping {
		attributes = scratch
	}

	message := pbEntryPool.Get().(*filer_pb.Entry)
	defer func() {
		resetPbEntry(message)
		pbEntryPool.Put(message)
	}()

	if err := proto.Unmarshal(attributes, message); err != nil {
		return fmt.Errorf("decoding value blob for %s: %v", entry.FullPath, err)
	}

	FromPbEntryToExistingEntry(message, entry)

	// FromPbEntryToExistingEntry took the size over a chunk list that is not
	// there, so fold in what the chunks actually reached. This is TotalSize.
	if chunkExtent > entry.FileSize {
		entry.FileSize = chunkExtent
	}

	return nil
}

// chunkExtentEnd reports where one encoded FileChunk ends, the offset plus size
// that TotalSize maximises over, without building the chunk.
func chunkExtentEnd(chunk []byte) (uint64, error) {
	var offset int64
	var size uint64
	for len(chunk) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(chunk)
		if tagLen < 0 {
			return 0, protowire.ParseError(tagLen)
		}
		chunk = chunk[tagLen:]
		if typ == protowire.VarintType && (num == fileChunkOffsetField || num == fileChunkSizeField) {
			v, n := protowire.ConsumeVarint(chunk)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			if num == fileChunkOffsetField {
				offset = int64(v)
			} else {
				size = v
			}
			chunk = chunk[n:]
			continue
		}
		n := protowire.ConsumeFieldValue(num, typ, chunk)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		chunk = chunk[n:]
	}
	return uint64(offset + int64(size)), nil
}
