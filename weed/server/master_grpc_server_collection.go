package weed_server

import (
	"context"
	"time"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// deleteCollectionTimeout bounds one DeleteCollection RPC to a volume server, so
// a server that accepts the connection and then stops answering cannot hold the
// fan-out open with nothing to end it. Same bound as allocateVolumeTimeout, the
// other master-to-volume-server admin RPC. The volume server runs the delete to
// completion regardless of the request context, so giving up costs the
// confirmation and not the deletion.
const deleteCollectionTimeout = 1 * time.Minute

func (ms *MasterServer) CollectionList(ctx context.Context, req *master_pb.CollectionListRequest) (*master_pb.CollectionListResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.CollectionListResponse{}
	collections := ms.Topo.ListCollections(req.IncludeNormalVolumes, req.IncludeEcVolumes)
	for _, c := range collections {
		resp.Collections = append(resp.Collections, &master_pb.Collection{
			Name: c,
		})
	}

	return resp, nil
}

func (ms *MasterServer) CollectionDelete(ctx context.Context, req *master_pb.CollectionDeleteRequest) (*master_pb.CollectionDeleteResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if err := ms.deleteCollection(ctx, req.Name); err != nil {
		return nil, err
	}

	return &master_pb.CollectionDeleteResponse{}, nil
}

// deleteCollection removes a collection's normal volumes and its EC shards. Both
// passes run: one collection can hold both, and returning after a failed normal
// pass left the shards in place with no request left to come back for them. The
// normal pass keeps precedence in what is reported, as it did before.
func (ms *MasterServer) deleteCollection(ctx context.Context, collectionName string) error {

	// Values only, no deadline: a caller that hangs up must not abandon a
	// destructive fan-out part-done, with volumes left behind and no request still
	// running to come back for them. Each RPC is bounded on its own.
	ctx = context.WithoutCancel(ctx)

	normalErr := ms.doDeleteNormalCollection(ctx, collectionName)
	ecErr := ms.doDeleteEcCollection(ctx, collectionName)

	if normalErr != nil {
		if ecErr != nil {
			glog.ErrorfCtx(ctx, "delete collection %s ec shards: %v", collectionName, ecErr)
		}
		return normalErr
	}

	return ecErr
}

func (ms *MasterServer) doDeleteNormalCollection(ctx context.Context, collectionName string) error {

	collection, ok := ms.Topo.FindCollection(collectionName)
	if !ok {
		return nil
	}

	// One RPC per server, not one per replica: ListVolumeServers reports a node
	// once for every replica it holds, while DeleteCollection removes the whole
	// collection from the server it reaches. ListEcServersByCollection already
	// returns each server once.
	var servers []pb.ServerAddress
	seen := make(map[pb.ServerAddress]struct{})
	for _, node := range collection.ListVolumeServers() {
		address := node.ServerAddress()
		if _, done := seen[address]; done {
			continue
		}
		seen[address] = struct{}{}
		servers = append(servers, address)
	}

	if err := ms.deleteCollectionFrom(ctx, collectionName, servers); err != nil {
		return err
	}
	ms.Topo.DeleteCollection(collectionName)

	return nil
}

func (ms *MasterServer) doDeleteEcCollection(ctx context.Context, collectionName string) error {

	if err := ms.deleteCollectionFrom(ctx, collectionName, ms.Topo.ListEcServersByCollection(collectionName)); err != nil {
		return err
	}
	ms.Topo.DeleteEcCollection(collectionName)

	return nil
}

// deleteCollectionFrom asks every server to drop the collection and keeps going
// past a failure, so one server that is down does not leave the collection on
// every server after it in the list. The first failure is what is reported, and
// the collection stays in the topology so a later delete comes back for the rest.
func (ms *MasterServer) deleteCollectionFrom(ctx context.Context, collectionName string, servers []pb.ServerAddress) error {

	var firstErr error
	for _, server := range servers {
		err := operation.WithVolumeServerClient(false, server, ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			rpcCtx, cancel := context.WithTimeout(ctx, deleteCollectionTimeout)
			defer cancel()
			_, deleteErr := client.DeleteCollection(rpcCtx, &volume_server_pb.DeleteCollectionRequest{
				Collection: collectionName,
			})
			return deleteErr
		})
		if err != nil {
			glog.ErrorfCtx(ctx, "delete collection %s on %s: %v", collectionName, server, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}
