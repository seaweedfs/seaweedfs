package weed_server

import (
	"context"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// metadataSubscriber is the live record of one connected metadata subscriber
// (a FUSE mount, S3, filer.sync, peer filer, ...). It is created in addClient
// and removed in deleteClient, so the registry reflects currently-connected
// clients only. Guarded by FilerServer.knownListenersLock.
type metadataSubscriber struct {
	clientName    string // "<type>@<address>"
	clientType    string // e.g. "mount", "sw-vfs", "s3", "filer:<addr>"
	address       string
	pathPrefix    string
	clientId      int32
	clientEpoch   int32
	connectedAtNs int64
}

// ListMetadataSubscribers returns the metadata subscribers currently connected
// to this filer, optionally filtered by client type. Each filer only knows its
// own subscribers, so cluster-wide callers query every filer and aggregate.
func (fs *FilerServer) ListMetadataSubscribers(ctx context.Context, req *filer_pb.ListMetadataSubscribersRequest) (*filer_pb.ListMetadataSubscribersResponse, error) {
	typeFilter := make(map[string]bool, len(req.ClientTypes))
	for _, t := range req.ClientTypes {
		typeFilter[t] = true
	}

	filerAddress := string(fs.option.Host)

	fs.knownListenersLock.Lock()
	subs := make([]*filer_pb.MetadataSubscriber, 0, len(fs.subscribers))
	for _, s := range fs.subscribers {
		if len(typeFilter) > 0 && !typeFilter[s.clientType] {
			continue
		}
		subs = append(subs, &filer_pb.MetadataSubscriber{
			ClientName:    s.clientName,
			ClientType:    s.clientType,
			Address:       s.address,
			PathPrefix:    s.pathPrefix,
			ClientId:      s.clientId,
			ClientEpoch:   s.clientEpoch,
			ConnectedAtNs: s.connectedAtNs,
			FilerAddress:  filerAddress,
		})
	}
	fs.knownListenersLock.Unlock()

	// Stable ordering so the admin page doesn't shuffle rows between refreshes.
	sort.Slice(subs, func(i, j int) bool {
		if subs[i].ClientName != subs[j].ClientName {
			return subs[i].ClientName < subs[j].ClientName
		}
		return subs[i].PathPrefix < subs[j].PathPrefix
	})

	return &filer_pb.ListMetadataSubscribersResponse{Subscribers: subs}, nil
}
