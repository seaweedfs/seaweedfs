package remote_storage

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

const (
	SyncKeyPrefix = "remote.sync."
)

func GetSyncOffset(grpcDialOption grpc.DialOption, filer pb.ServerAddress, dir string) (lastOffsetTsNs int64, readErr error) {

	dirHash := uint32(util.HashStringToLong(dir))

	readErr = pb.WithFilerClient(false, 0, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		syncKey := []byte(SyncKeyPrefix + "____")
		util.Uint32toBytes(syncKey[len(SyncKeyPrefix):len(SyncKeyPrefix)+4], dirHash)

		resp, err := client.KvGet(context.Background(), &filer_pb.KvGetRequest{Key: syncKey})
		if err != nil {
			return err
		}

		if len(resp.Error) != 0 {
			return errors.New(resp.Error)
		}
		if len(resp.Value) < 8 {
			return nil
		}

		lastOffsetTsNs = int64(util.BytesToUint64(resp.Value))

		return nil
	})

	return

}

func SetSyncOffset(grpcDialOption grpc.DialOption, filer pb.ServerAddress, dir string, offsetTsNs int64) error {

	dirHash := uint32(util.HashStringToLong(dir))

	return pb.WithFilerClient(false, 0, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		syncKey := []byte(SyncKeyPrefix + "____")
		util.Uint32toBytes(syncKey[len(SyncKeyPrefix):len(SyncKeyPrefix)+4], dirHash)

		valueBuf := make([]byte, 8)
		util.Uint64toBytes(valueBuf, uint64(offsetTsNs))

		resp, err := client.KvPut(context.Background(), &filer_pb.KvPutRequest{
			Key:   syncKey,
			Value: valueBuf,
		})
		if err != nil {
			return err
		}

		if len(resp.Error) != 0 {
			return errors.New(resp.Error)
		}

		return nil

	})

}
