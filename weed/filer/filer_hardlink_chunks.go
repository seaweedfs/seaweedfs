package filer

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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
	return replacesHardLink(replaced, replacement) && !f.hardLinkIsOrphaned(ctx, replaced.HardLinkId)
}

// replacesHardLink reports whether replacement unlinks a hard-linked
// replaced; the same file written through one of its names does not: the
// store rewrote the shared record, and no name reads what the write left out.
func replacesHardLink(replaced, replacement *Entry) bool {
	if replaced == nil || len(replaced.HardLinkId) == 0 {
		return false
	}
	return replacement == nil || !bytes.Equal(replaced.HardLinkId, replacement.HardLinkId)
}

// KeepHardLinkedChunks takes out of garbage, the chunks a write is about to
// delete, every chunk of replaced's file while another hard link still reads
// it. The chunks come from the shared record, since replaced may be a stale
// row when its lookup could not read the record.
//
// When the record or the manifests cannot be read nothing is deleted: a
// leaked chunk can be collected later, a deleted one that a name still reads
// is lost.
func (f *Filer) KeepHardLinkedChunks(ctx context.Context, replaced, replacement *Entry, garbage []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	if len(garbage) == 0 || !replacesHardLink(replaced, replacement) {
		return garbage
	}
	value, err := f.Store.KvGet(ctx, replaced.HardLinkId)
	if errors.Is(err, ErrKvNotFound) {
		return garbage
	}
	shared := &Entry{}
	if err == nil {
		err = shared.DecodeAttributesAndChunks(value)
	}
	if err == nil {
		var kept []*filer_pb.FileChunk
		kept, err = withoutChunksOf(ctx, f.MasterClient.GetLookupFileIdFunction(), f.MasterClient, append(shared.GetChunks(), replaced.GetChunks()...), garbage)
		if err == nil {
			return kept
		}
	}
	glog.ErrorfCtx(ctx, "keep %d chunk(s) of %s, which another hard link may still read: %v", len(garbage), replaced.FullPath, err)
	return nil
}

// withoutChunksOf returns garbage without chunks, without the manifests among
// them, and without every chunk those manifests list, nested ones included.
func withoutChunksOf(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, invalidator CacheInvalidator, chunks, garbage []*filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
	data, manifests, err := ResolveChunkManifest(ctx, lookupFileIdFn, chunks, 0, math.MaxInt64, invalidator)
	if err != nil {
		return nil, err
	}
	return DoMinusChunks(garbage, append(data, manifests...)), nil
}
