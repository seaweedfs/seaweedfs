package filer

import (
	"bytes"
	"context"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// hardLinkStillHeld reports whether replaced, the entry a write has just
// replaced at its path, was one of several hard links to a file another name
// still reads: replaced carried a hard link, its replacement is not that same
// link, and the file's shared record outlived the drop of replaced's name.
//
// Ask it after the write. The store drops the replaced name's link as part of
// the write, so the shared record is gone exactly when replaced was the last
// link, whatever link count replaced carried when it was read.
func (f *Filer) hardLinkStillHeld(ctx context.Context, replaced, replacement *Entry) bool {
	if replaced == nil || len(replaced.HardLinkId) == 0 {
		return false
	}
	if replacement != nil && bytes.Equal(replaced.HardLinkId, replacement.HardLinkId) {
		// the same file written through one of its names: the store rewrote
		// the shared record, and no name reads what the write left out
		return false
	}
	return !f.hardLinkIsOrphaned(ctx, replaced.HardLinkId)
}

// KeepHardLinkedChunks takes out of garbage, the chunks a write is about to
// delete, every chunk of replaced, the entry the write replaced, while
// another hard link to replaced's file still reads it. A write over one name
// of a hard-linked file unlinks that name only; the file's chunks belong to
// the names left.
//
// When replaced's manifests cannot be resolved nothing is deleted: a leaked
// chunk can be collected later, a deleted one that a name still reads is lost.
func (f *Filer) KeepHardLinkedChunks(ctx context.Context, replaced, replacement *Entry, garbage []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	if len(garbage) == 0 || !f.hardLinkStillHeld(ctx, replaced, replacement) {
		return garbage
	}
	data, manifests, err := ResolveChunkManifest(ctx, f.MasterClient.GetLookupFileIdFunction(), replaced.GetChunks(), 0, math.MaxInt64, f.MasterClient)
	if err != nil {
		glog.ErrorfCtx(ctx, "keep %d chunk(s) of %s, which another hard link still reads: resolve its chunks: %v", len(garbage), replaced.FullPath, err)
		return nil
	}
	return DoMinusChunks(garbage, append(data, manifests...))
}
