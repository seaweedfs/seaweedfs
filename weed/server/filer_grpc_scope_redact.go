package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

func scopeEntryNeedsRedaction(entry *filer_pb.Entry) bool {
	if entry == nil {
		return false
	}
	_, ok := entry.GetExtended()[filer.ExtRemoteRenameSourceKey]
	return ok
}

func redactScopeEntry(entry *filer_pb.Entry) {
	if entry != nil {
		delete(entry.GetExtended(), filer.ExtRemoteRenameSourceKey)
	}
}

// scopePreserveRemoteRenameSource keeps the filer-owned source marker on a
// scoped replacement when the client could not see it in its redacted read.
// The caller holds the entry path lock; copy the bytes so the stored entry and
// request never share mutable backing storage.
func scopePreserveRemoteRenameSource(stored, replacement *filer.Entry) {
	if stored == nil || replacement == nil {
		return
	}
	marker, ok := stored.Extended[filer.ExtRemoteRenameSourceKey]
	if !ok {
		return
	}
	if _, supplied := replacement.Extended[filer.ExtRemoteRenameSourceKey]; !supplied {
		extended := make(map[string][]byte, len(replacement.Extended)+1)
		for key, value := range replacement.Extended {
			extended[key] = append([]byte(nil), value...)
		}
		extended[filer.ExtRemoteRenameSourceKey] = append([]byte(nil), marker...)
		replacement.Extended = extended
	}
}

func redactScopeMetadataResponseInPlace(resp *filer_pb.SubscribeMetadataResponse) {
	if resp == nil {
		return
	}
	if notification := resp.GetEventNotification(); notification != nil {
		redactScopeEntry(notification.GetOldEntry())
		redactScopeEntry(notification.GetNewEntry())
	}
	for _, nested := range resp.GetEvents() {
		redactScopeMetadataResponseInPlace(nested)
	}
}

// scopeRedactLookupResponse clones a scoped lookup response only when the
// filer-owned remote rename marker is present. The handler may have returned
// an entry backed by the filer store, so the response must not be edited in
// place.
func scopeRedactLookupResponse(resp *filer_pb.LookupDirectoryEntryResponse) *filer_pb.LookupDirectoryEntryResponse {
	if resp == nil || !scopeEntryNeedsRedaction(resp.GetEntry()) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.LookupDirectoryEntryResponse)
	redactScopeEntry(out.GetEntry())
	return out
}

// scopeRedactBatchLookupResponse applies the same rule to every positional
// result while preserving the response shape and all other entry metadata.
func scopeRedactBatchLookupResponse(resp *filer_pb.LookupDirectoryEntriesResponse) *filer_pb.LookupDirectoryEntriesResponse {
	if resp == nil {
		return nil
	}
	needsRedaction := false
	for _, result := range resp.GetResults() {
		if result != nil && scopeEntryNeedsRedaction(result.GetEntry()) {
			needsRedaction = true
			break
		}
	}
	if !needsRedaction {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.LookupDirectoryEntriesResponse)
	for _, result := range out.GetResults() {
		if result != nil {
			redactScopeEntry(result.GetEntry())
		}
	}
	return out
}

func scopeRedactListResponse(resp *filer_pb.ListEntriesResponse) *filer_pb.ListEntriesResponse {
	if resp == nil || !scopeEntryNeedsRedaction(resp.GetEntry()) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.ListEntriesResponse)
	redactScopeEntry(out.GetEntry())
	return out
}

func scopeMetadataNeedsRedaction(resp *filer_pb.SubscribeMetadataResponse) bool {
	if resp == nil {
		return false
	}
	if notification := resp.GetEventNotification(); notification != nil {
		if scopeEntryNeedsRedaction(notification.GetOldEntry()) || scopeEntryNeedsRedaction(notification.GetNewEntry()) {
			return true
		}
	}
	for _, nested := range resp.GetEvents() {
		if scopeMetadataNeedsRedaction(nested) {
			return true
		}
	}
	return false
}

// scopeRedactMetadataResponse recursively clones a subscription response so
// old and new entries in both the head and batched Events cannot mutate the
// source log or filer state while the internal marker is removed.
func scopeRedactMetadataResponse(resp *filer_pb.SubscribeMetadataResponse) *filer_pb.SubscribeMetadataResponse {
	if resp == nil || !scopeMetadataNeedsRedaction(resp) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
	redactScopeMetadataResponseInPlace(out)
	return out
}

func scopeRedactCreateResponse(resp *filer_pb.CreateEntryResponse) *filer_pb.CreateEntryResponse {
	if resp == nil || !scopeMetadataNeedsRedaction(resp.GetMetadataEvent()) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.CreateEntryResponse)
	redactScopeMetadataResponseInPlace(out.GetMetadataEvent())
	return out
}

func scopeRedactUpdateResponse(resp *filer_pb.UpdateEntryResponse) *filer_pb.UpdateEntryResponse {
	if resp == nil || !scopeMetadataNeedsRedaction(resp.GetMetadataEvent()) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.UpdateEntryResponse)
	redactScopeMetadataResponseInPlace(out.GetMetadataEvent())
	return out
}

func scopeRedactDeleteResponse(resp *filer_pb.DeleteEntryResponse) *filer_pb.DeleteEntryResponse {
	if resp == nil || !scopeMetadataNeedsRedaction(resp.GetMetadataEvent()) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.DeleteEntryResponse)
	redactScopeMetadataResponseInPlace(out.GetMetadataEvent())
	return out
}

func scopeRenameResponseNeedsRedaction(resp *filer_pb.StreamRenameEntryResponse) bool {
	if resp == nil || resp.GetEventNotification() == nil {
		return false
	}
	return scopeEntryNeedsRedaction(resp.GetEventNotification().GetOldEntry()) ||
		scopeEntryNeedsRedaction(resp.GetEventNotification().GetNewEntry())
}

func scopeRedactRenameResponse(resp *filer_pb.StreamRenameEntryResponse) *filer_pb.StreamRenameEntryResponse {
	if !scopeRenameResponseNeedsRedaction(resp) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.StreamRenameEntryResponse)
	redactScopeEntry(out.GetEventNotification().GetOldEntry())
	redactScopeEntry(out.GetEventNotification().GetNewEntry())
	return out
}

func scopeMutateResponseNeedsRedaction(resp *filer_pb.StreamMutateEntryResponse) bool {
	if resp == nil {
		return false
	}
	return scopeMetadataNeedsRedaction(resp.GetCreateResponse().GetMetadataEvent()) ||
		scopeMetadataNeedsRedaction(resp.GetUpdateResponse().GetMetadataEvent()) ||
		scopeMetadataNeedsRedaction(resp.GetDeleteResponse().GetMetadataEvent()) ||
		scopeRenameResponseNeedsRedaction(resp.GetRenameResponse())
}

func scopeRedactMutateResponse(resp *filer_pb.StreamMutateEntryResponse) *filer_pb.StreamMutateEntryResponse {
	if !scopeMutateResponseNeedsRedaction(resp) {
		return resp
	}
	out := proto.Clone(resp).(*filer_pb.StreamMutateEntryResponse)
	redactScopeMetadataResponseInPlace(out.GetCreateResponse().GetMetadataEvent())
	redactScopeMetadataResponseInPlace(out.GetUpdateResponse().GetMetadataEvent())
	redactScopeMetadataResponseInPlace(out.GetDeleteResponse().GetMetadataEvent())
	if rename := out.GetRenameResponse(); rename != nil && rename.GetEventNotification() != nil {
		redactScopeEntry(rename.GetEventNotification().GetOldEntry())
		redactScopeEntry(rename.GetEventNotification().GetNewEntry())
	}
	return out
}

func scopeRedactResponse(resp any) any {
	switch out := resp.(type) {
	case *filer_pb.LookupDirectoryEntryResponse:
		return scopeRedactLookupResponse(out)
	case *filer_pb.LookupDirectoryEntriesResponse:
		return scopeRedactBatchLookupResponse(out)
	case *filer_pb.ListEntriesResponse:
		return scopeRedactListResponse(out)
	case *filer_pb.SubscribeMetadataResponse:
		return scopeRedactMetadataResponse(out)
	case *filer_pb.CreateEntryResponse:
		return scopeRedactCreateResponse(out)
	case *filer_pb.UpdateEntryResponse:
		return scopeRedactUpdateResponse(out)
	case *filer_pb.DeleteEntryResponse:
		return scopeRedactDeleteResponse(out)
	case *filer_pb.StreamRenameEntryResponse:
		return scopeRedactRenameResponse(out)
	case *filer_pb.StreamMutateEntryResponse:
		return scopeRedactMutateResponse(out)
	default:
		return resp
	}
}
