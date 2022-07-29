package weed_server

import (
	"context"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
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

	err := ms.doDeleteNormalCollection(req.Name)

	if err != nil {
		return nil, err
	}

	err = ms.doDeleteEcCollection(req.Name)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (ms *MasterServer) doDeleteNormalCollection(collectionName string) error {

	collection, ok := ms.Topo.FindCollection(collectionName)
	if !ok {
		return nil
	}

	for _, server := range collection.ListVolumeServers() {
		err := operation.WithVolumeServerClient(false, server.ServerAddress(), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
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

func (ms *MasterServer) doDeleteEcCollection(collectionName string) error {

	listOfEcServers := ms.Topo.ListEcServersByCollection(collectionName)

	for _, server := range listOfEcServers {
		err := operation.WithVolumeServerClient(false, server, ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
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
