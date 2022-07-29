package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func (fs *FilerServer) KvGet(ctx context.Context, req *filer_pb.KvGetRequest) (*filer_pb.KvGetResponse, error) {

	value, err := fs.filer.Store.KvGet(ctx, req.Key)
	if err == filer.ErrKvNotFound {
		return &filer_pb.KvGetResponse{}, nil
	}

	if err != nil {
		return &filer_pb.KvGetResponse{Error: err.Error()}, nil
	}

	return &filer_pb.KvGetResponse{
		Value: value,
	}, nil

}

// KvPut sets the key~value. if empty value, delete the kv entry
func (fs *FilerServer) KvPut(ctx context.Context, req *filer_pb.KvPutRequest) (*filer_pb.KvPutResponse, error) {

	if len(req.Value) == 0 {
		if err := fs.filer.Store.KvDelete(ctx, req.Key); err != nil {
			return &filer_pb.KvPutResponse{Error: err.Error()}, nil
		}
	}

	err := fs.filer.Store.KvPut(ctx, req.Key, req.Value)
	if err != nil {
		return &filer_pb.KvPutResponse{Error: err.Error()}, nil
	}

	return &filer_pb.KvPutResponse{}, nil

}
