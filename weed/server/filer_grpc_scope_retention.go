package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scopeRetainsData is used at admission and again under the handler's path
// lock. A new chunk requires a live assignment proof; retaining one requires
// it still to be stored at the target. Inline bytes need no volume capability.
// Shared-store transactions remain a separate concern.
func scopeRetainsData(stored, requested *filer_pb.Entry, proven func(string) bool) error {
	if err := scopeCheckEntryShape(requested); err != nil {
		return err
	}
	known := make(map[string]bool, len(stored.GetChunks()))
	identities := make(map[string]string, len(stored.GetChunks())+len(requested.GetChunks()))
	for _, chunk := range stored.GetChunks() {
		if chunk != nil {
			fid := chunk.GetFileIdString()
			known[fid] = true
			identity := scopeChunkIdentity(fid)
			if old, exists := identities[identity]; exists && old != fid {
				// Existing aliases cannot safely be retained while one of their
				// spellings might be queued for deletion.
				identities[identity] = ""
			} else {
				identities[identity] = fid
			}
		}
	}
	for _, chunk := range requested.GetChunks() {
		fid := chunk.GetFileIdString()
		identity := scopeChunkIdentity(fid)
		if old, exists := identities[identity]; exists && old != fid {
			return fmt.Errorf("chunk identity aliases are not allowed: %s", fid)
		}
		// Garbage comparisons use literal IDs. A proof must not admit a
		// second spelling of the same needle, even within a new chunk list.
		identities[identity] = fid
		if !known[fid] && (proven == nil || !proven(fid)) {
			return fmt.Errorf("%s %s", filer_pb.ChunkProofRefused, fid)
		}
	}
	return nil
}

func scopeChunkIdentity(fid string) string {
	volume, path, ok := strings.Cut(fid, ",")
	if !ok {
		return fid
	}
	vid, err := needle.NewVolumeId(volume)
	if err != nil {
		return fid
	}
	var n needle.Needle
	if err := n.ParsePath(path); err != nil {
		return fid
	}
	return (&needle.FileId{VolumeId: vid, Key: n.Id, Cookie: n.Cookie}).String()
}

// scopeKeepsHardLink refuses a write that changes a stored hard link's count
// or keeps its chunks on a name detached from it. Either way a later delete of
// this name would delete chunks that another name of the file still reads.
func scopeKeepsHardLink(stored, requested *filer_pb.Entry) error {
	id := stored.GetHardLinkId()
	if len(id) == 0 {
		return nil
	}
	if bytes.Equal(requested.GetHardLinkId(), id) {
		if requested.GetHardLinkCounter() != stored.GetHardLinkCounter() {
			return fmt.Errorf("the link count of hard link %x cannot be changed", id)
		}
		return nil
	}
	if len(requested.GetChunks()) > 0 {
		return fmt.Errorf("the chunks of hard link %x cannot be detached from it", id)
	}
	return nil
}

func checkScopeRetainedData(ctx context.Context, stored *filer.Entry, requested *filer_pb.Entry, chunkAuth map[string]string) error {
	if !scopeKeepOnly(ctx) {
		return nil
	}
	var current *filer_pb.Entry
	if stored != nil {
		current = stored.ToProtoEntry()
	}
	var proven func(string) bool
	if authority, ok := ctx.Value(scopeWriteAuthorityKey{}).(*scopeWriteAuthority); ok {
		if authority.sc.expired() {
			return scopeExpiredError(authority.sc)
		}
		proven = func(fid string) bool { return authority.s.proven(authority.sc, chunkAuth, fid) }
	}
	if err := scopeRetainsData(current, requested, proven); err != nil {
		return status.Errorf(codes.PermissionDenied, "filer: retained data changed before commit: %v", err)
	}
	if err := scopeKeepsHardLink(current, requested); err != nil {
		return status.Errorf(codes.PermissionDenied, "filer: %v", err)
	}
	return nil
}
