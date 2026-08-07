package filer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Field numbers from filer.proto. Only the ones this decoder has to recognise
// are named; every other Entry field is handed to the generated unmarshaller
// as-is.
const (
	entryChunksField     = 3 // Entry.chunks
	fileChunkOffsetField = 2 // FileChunk.offset
	fileChunkSizeField   = 3 // FileChunk.size
)

// The chunk bytes are the one part of the blob the generated unmarshaller never
// sees, so the checks it would have made are made here instead: a submessage
// has to parse, and a proto3 string has to be valid UTF-8. Anything else in a
// FileChunk is a scalar, which walking it already validates.
// TestChunkValidationCoversEveryField fails if FileChunk gains a field of
// either kind that is missing from these.
var (
	fileChunkMessageFields = map[protowire.Number]bool{
		7: true, // fid, a FileId of scalars only, so walking it is a full check
		8: true, // source_fid
	}
	fileChunkStringFields = map[protowire.Number]bool{
		1: true, // file_id
		5: true, // e_tag
		6: true, // source_file_id
	}
)

// attributesScratchPool holds the re-encoded entry, which is the blob minus its
// chunks and so much smaller than what came in.
var attributesScratchPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

// DecodeListedEntry decodes one listed entry, dropping the chunk list when the
// listing asked for attributes only.
func DecodeListedEntry(ctx context.Context, entry *Entry, blob []byte) error {
	if filer_pb.ChunksOmitted(ctx) {
		return entry.DecodeAttributesOnly(blob)
	}
	return entry.DecodeAttributesAndChunks(blob)
}

// DecodeAttributesOnly fills entry from blob without building its chunk list,
// which is the bulk of the work for anything but a tiny file. Chunks are still
// measured, because an entry whose stored FileSize is zero takes its size from
// them, but no FileChunk is allocated.
//
// entry.Chunks is left nil. The one exception is a hard link, whose attributes
// and chunks are replaced wholesale by a full decode of its own record in
// FilerStoreWrapper.maybeReadHardLink straight after the store listing. Only a
// caller that reads attributes and nothing else may use this.
func (entry *Entry) DecodeAttributesOnly(blob []byte) error {
	// The scratch buffer is only taken once a chunk is actually found, so an
	// entry with none — every directory, for one — neither re-encodes nor
	// touches the pool, and is unmarshalled where it lies.
	var scratchPtr *[]byte
	var scratch []byte
	defer func() {
		if scratchPtr != nil {
			*scratchPtr = scratch
			attributesScratchPool.Put(scratchPtr)
		}
	}()

	var chunkExtent uint64
	attributes := blob
	for pos := 0; pos < len(blob); {
		rest := blob[pos:]
		num, typ, tagLen := protowire.ConsumeTag(rest)
		if tagLen < 0 {
			return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, protowire.ParseError(tagLen))
		}
		var valLen int
		if num == entryChunksField && typ == protowire.BytesType {
			chunk, n := protowire.ConsumeBytes(rest[tagLen:])
			if n < 0 {
				return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, protowire.ParseError(n))
			}
			valLen = n
			end, err := chunkExtentEnd(chunk)
			if err != nil {
				return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, err)
			}
			if end > chunkExtent {
				chunkExtent = end
			}
			if scratchPtr == nil {
				scratchPtr = attributesScratchPool.Get().(*[]byte)
				scratch = append((*scratchPtr)[:0], blob[:pos]...)
			}
		} else {
			valLen = protowire.ConsumeFieldValue(num, typ, rest[tagLen:])
			if valLen < 0 {
				return fmt.Errorf("decoding value blob for %s: %w", entry.FullPath, protowire.ParseError(valLen))
			}
			if scratchPtr != nil {
				scratch = append(scratch, rest[:tagLen+valLen]...)
			}
		}
		pos += tagLen + valLen
	}
	if scratchPtr != nil {
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
// that TotalSize maximises over, without building the chunk. A chunk this
// rejects is one the full decoder rejects too, so a listing never reports a
// size for an entry that cannot be opened.
//
// A manifest chunk needs no special handling: it carries the offset and size of
// the whole range it stands for, and TotalSize does not resolve it either.
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
		if typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(chunk)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			if fileChunkMessageFields[num] {
				if err := validateMessage(v); err != nil {
					return 0, err
				}
			} else if fileChunkStringFields[num] && !utf8.Valid(v) {
				return 0, errors.New("invalid UTF-8 in string field")
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

// validateMessage walks an encoded message to check it parses, which is all the
// generated unmarshaller would do for one whose fields are scalars.
func validateMessage(b []byte) error {
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}
		valLen := protowire.ConsumeFieldValue(num, typ, b[tagLen:])
		if valLen < 0 {
			return protowire.ParseError(valLen)
		}
		b = b[tagLen+valLen:]
	}
	return nil
}
