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
// fan-out open forever with nothing to end it (issue #7234). Same bound as
// allocateVolumeTimeout in weed/topology, the other master-to-volume-server
// admin RPC.
//
// This bounds each RPC and deliberately not the fan-out as a whole: the loop
// below runs the per-server contexts through context.WithoutCancel, keeping the
// caller's values and dropping its deadline. A collection can span many servers
// and the deletes are sequential, so laying a caller's budget over the whole
// loop would abandon a large delete part-done and skip the EC pass with it,
// leaving shards behind and no request left to retry them.
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
// passes always run: one collection can hold both, and returning after a failed
// normal pass left the EC shards in place with no request left to come back for
// them. The normal pass keeps precedence in what is reported, as it did before.
func (ms *MasterServer) deleteCollection(ctx context.Context, collectionName string) error {

	normalErr := ms.doDeleteNormalCollection(ctx, collectionName)
	ecErr := ms.doDeleteEcCollection(ctx, collectionName)

	if normalErr != nil {
		if ecErr != nil {
			glog.ErrorfCtx(ctx, "delete collection %s: ec shards also failed: %v", collectionName, ecErr)
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

	// Values only, no deadline: see deleteCollectionTimeout.
	fanoutCtx := context.WithoutCancel(ctx)

	// One RPC per server rather than one per replica. ListVolumeServers reports a
	// node once for every replica of the collection it holds, and DeleteCollection
	// removes the whole collection from the server it reaches, so a collection with
	// thousands of volumes would otherwise repeat the same whole-collection delete
	// thousands of times against the same handful of nodes.
	// ListEcServersByCollection already returns each server once.
	deleted := make(map[pb.ServerAddress]struct{})
	for _, server := range collection.ListVolumeServers() {
		address := server.ServerAddress()
		if _, done := deleted[address]; done {
			continue
		}
		deleted[address] = struct{}{}

		err := operation.WithVolumeServerClient(false, address, ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			rpcCtx, cancel := context.WithTimeout(fanoutCtx, deleteCollectionTimeout)
			defer cancel()
			_, deleteErr := client.DeleteCollection(rpcCtx, &volume_server_pb.DeleteCollectionRequest{
				Collection: collectionName,
			})
			return deleteErr
		})
		if err != nil {
			return err
		}
	}
	ms.Topo.DeleteCollection(collectionName)

	return nil
}

func (ms *MasterServer) doDeleteEcCollection(ctx context.Context, collectionName string) error {

	listOfEcServers := ms.Topo.ListEcServersByCollection(collectionName)

	// Values only, no deadline: see deleteCollectionTimeout.
	fanoutCtx := context.WithoutCancel(ctx)

	for _, server := range listOfEcServers {
		err := operation.WithVolumeServerClient(false, server, ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			rpcCtx, cancel := context.WithTimeout(fanoutCtx, deleteCollectionTimeout)
			defer cancel()
			_, deleteErr := client.DeleteCollection(rpcCtx, &volume_server_pb.DeleteCollectionRequest{
				Collection: collectionName,
			})
			return deleteErr
		})
		if err != nil {
			return err
		}
	}

	ms.Topo.DeleteEcCollection(collectionName)

	return nil
}
