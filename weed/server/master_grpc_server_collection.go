package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

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

	resp := &master_pb.CollectionDeleteResponse{}

	collection, ok := ms.Topo.FindCollection(req.GetName())
	if !ok {
		return resp, fmt.Errorf("collection not found: %v", req.GetName())
	}

	for _, server := range collection.ListVolumeServers() {
		err := operation.WithVolumeServerClient(server.Url(), ms.grpcDialOpiton, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collection.Name,
			})
			return deleteErr
		})
		if err != nil {
			return nil, err
		}
	}
	ms.Topo.DeleteCollection(req.GetName())

	return resp, nil
}
