package weed_server

import (
	"bytes"
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scopeRetainsData is used at admission and again under the handler's path
// lock. This prevents queued mutations on this filer from reviving content
// removed after admission. Shared-store transactions remain a separate concern.
func scopeRetainsData(stored, requested *filer_pb.Entry) error {
	if len(requested.GetContent()) > 0 && !bytes.Equal(requested.GetContent(), stored.GetContent()) {
		return fmt.Errorf("new inline content is not allowed")
	}
	known := make(map[string]bool, len(stored.GetChunks()))
	for _, chunk := range stored.GetChunks() {
		if chunk != nil {
			known[chunk.GetFileIdString()] = true
		}
	}
	for _, chunk := range requested.GetChunks() {
		if chunk == nil || !known[chunk.GetFileIdString()] {
			return fmt.Errorf("new chunk file ids are not allowed")
		}
	}
	return nil
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

func checkScopeRetainedData(ctx context.Context, stored *filer.Entry, requested *filer_pb.Entry) error {
	if !scopeKeepOnly(ctx) {
		return nil
	}
	var current *filer_pb.Entry
	if stored != nil {
		current = stored.ToProtoEntry()
	}
	if err := scopeRetainsData(current, requested); err != nil {
		return status.Errorf(codes.PermissionDenied, "filer: retained data changed before commit: %v", err)
	}
	if err := scopeKeepsHardLink(current, requested); err != nil {
		return status.Errorf(codes.PermissionDenied, "filer: %v", err)
	}
	return nil
}
