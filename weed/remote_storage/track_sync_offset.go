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

// SyncOffsetKey is the filer store key holding the write-back sync watermark
// for a mounted directory.
func SyncOffsetKey(dir string) []byte {
	syncKey := make([]byte, len(SyncKeyPrefix)+4)
	copy(syncKey, SyncKeyPrefix)
	util.Uint32toBytes(syncKey[len(SyncKeyPrefix):], uint32(util.HashStringToLong(dir)))
	return syncKey
}

func GetSyncOffset(grpcDialOption grpc.DialOption, filer pb.ServerAddress, dir string) (lastOffsetTsNs int64, readErr error) {

	readErr = pb.WithFilerClient(false, 0, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.KvGet(context.Background(), &filer_pb.KvGetRequest{Key: SyncOffsetKey(dir)})
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

	return pb.WithFilerClient(false, 0, filer, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		valueBuf := make([]byte, 8)
		util.Uint64toBytes(valueBuf, uint64(offsetTsNs))

		resp, err := client.KvPut(context.Background(), &filer_pb.KvPutRequest{
			Key:   SyncOffsetKey(dir),
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
