package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func metadataSubscriptionScope(ctx context.Context, method string) ([]string, error) {
	v := util.GetViper()
	return security.CheckGrpcMetadataSubscription(ctx,
		security.SigningKey(v.GetString("jwt.filer_signing.subscribe.key")),
		security.SigningKey(v.GetString("jwt.filer_signing.key")),
		method)
}

// scopeMetadataResponse keeps only what a scoped subscriber may see: events
// whose every path is in scope, so a rename across the boundary never shows
// the far side, and no log file refs. Returns nil when nothing is left.
func scopeMetadataResponse(resp *filer_pb.SubscribeMetadataResponse, prefixes []string) *filer_pb.SubscribeMetadataResponse {
	if resp.EventNotification == nil && len(resp.LogFileRefs) > 0 {
		return nil
	}
	var kept []*filer_pb.SubscribeMetadataResponse
	for _, e := range resp.Events {
		if metadataEventInScope(e, prefixes) {
			kept = append(kept, e)
		}
	}
	headInScope := metadataEventInScope(resp, prefixes)
	if !headInScope && len(kept) == 0 {
		return nil
	}
	if headInScope && len(kept) == len(resp.Events) && len(resp.LogFileRefs) == 0 {
		return resp
	}
	scoped := &filer_pb.SubscribeMetadataResponse{
		TsNs:        resp.TsNs,
		Events:      kept,
		FlushedTsNs: resp.FlushedTsNs,
	}
	if headInScope {
		scoped.Directory = resp.Directory
		scoped.EventNotification = resp.EventNotification
		scoped.MutationTsNs = resp.MutationTsNs
	} else {
		// the batch head keeps its position as an empty marker
		scoped.EventNotification = &filer_pb.EventNotification{}
	}
	return scoped
}

func metadataEventInScope(event *filer_pb.SubscribeMetadataResponse, prefixes []string) bool {
	if event.EventNotification == nil || filer_pb.IsEmpty(event) {
		return true
	}
	return security.MetadataPathInScope(filer_pb.MetadataEventSourceFullPath(event), prefixes) &&
		security.MetadataPathInScope(filer_pb.MetadataEventTargetFullPath(event), prefixes)
}

type scopedSubscribeStream struct {
	filer_pb.SeaweedFiler_SubscribeMetadataServer
	prefixes []string
}

func (s *scopedSubscribeStream) Send(resp *filer_pb.SubscribeMetadataResponse) error {
	if scoped := scopeMetadataResponse(resp, s.prefixes); scoped != nil {
		return s.SeaweedFiler_SubscribeMetadataServer.Send(scoped)
	}
	return nil
}

type scopedSubscribeLocalStream struct {
	filer_pb.SeaweedFiler_SubscribeLocalMetadataServer
	prefixes []string
}

func (s *scopedSubscribeLocalStream) Send(resp *filer_pb.SubscribeMetadataResponse) error {
	if scoped := scopeMetadataResponse(resp, s.prefixes); scoped != nil {
		return s.SeaweedFiler_SubscribeLocalMetadataServer.Send(scoped)
	}
	return nil
}
