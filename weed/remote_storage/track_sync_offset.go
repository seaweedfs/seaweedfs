package remote_storage

import (
	"context"
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
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

// syncOffsetTimeout bounds an offset read or write: one that never answers
// would hold the daemon's start, or its event stream behind the checkpoint.
const syncOffsetTimeout = 30 * time.Second

func GetSyncOffset(grpcDialOption grpc.DialOption, filer pb.ServerAddress, dir string, tokenSource ...security.FilerTokenSource) (lastOffsetTsNs int64, readErr error) {

	readErr = pb.WithGrpcFilerClient(false, 0, filer, grpcDialOption, firstTokenSource(tokenSource), func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), syncOffsetTimeout)
		defer cancel()
		resp, err := client.KvGet(ctx, &filer_pb.KvGetRequest{Key: SyncOffsetKey(dir)})
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

func SetSyncOffset(grpcDialOption grpc.DialOption, filer pb.ServerAddress, dir string, offsetTsNs int64, tokenSource ...security.FilerTokenSource) error {

	return pb.WithGrpcFilerClient(false, 0, filer, grpcDialOption, firstTokenSource(tokenSource), func(client filer_pb.SeaweedFilerClient) error {

		valueBuf := make([]byte, 8)
		util.Uint64toBytes(valueBuf, uint64(offsetTsNs))

		ctx, cancel := context.WithTimeout(context.Background(), syncOffsetTimeout)
		defer cancel()
		resp, err := client.KvPut(ctx, &filer_pb.KvPutRequest{
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

func firstTokenSource(sources []security.FilerTokenSource) security.FilerTokenSource {
	if len(sources) == 0 {
		return nil
	}
	return sources[0]
}
