package pb

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"io"
	"time"
)

type EventErrorType int

const (
	TrivialOnError EventErrorType = iota
	FatalOnError
	RetryForeverOnError
	DontLogError
)

// MetadataFollowOption is used to control the behavior of the metadata following
// process. Part of it is used as a cursor to resume the following process.
type MetadataFollowOption struct {
	ClientName             string
	ClientId               int32
	ClientEpoch            int32
	SelfSignature          int32
	PathPrefix             string
	AdditionalPathPrefixes []string
	DirectoriesToWatch     []string
	StartTsNs              int64
	StopTsNs               int64
	EventErrorType         EventErrorType
}

type ProcessMetadataFunc func(resp *filer_pb.SubscribeMetadataResponse) error

func FollowMetadata(filerAddress ServerAddress, grpcDialOption grpc.DialOption, option *MetadataFollowOption, processEventFn ProcessMetadataFunc) error {

	err := WithFilerClient(true, option.SelfSignature, filerAddress, grpcDialOption, makeSubscribeMetadataFunc(option, processEventFn))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %v", err)
	}
	return err
}

func WithFilerClientFollowMetadata(filerClient filer_pb.FilerClient, option *MetadataFollowOption, processEventFn ProcessMetadataFunc) error {

	err := filerClient.WithFilerClient(true, makeSubscribeMetadataFunc(option, processEventFn))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %v", err)
	}

	return nil
}

func makeSubscribeMetadataFunc(option *MetadataFollowOption, processEventFn ProcessMetadataFunc) func(client filer_pb.SeaweedFilerClient) error {
	return func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:   option.ClientName,
			PathPrefix:   option.PathPrefix,
			PathPrefixes: option.AdditionalPathPrefixes,
			Directories:  option.DirectoriesToWatch,
			SinceNs:      option.StartTsNs,
			Signature:    option.SelfSignature,
			ClientId:     option.ClientId,
			ClientEpoch:  option.ClientEpoch,
			UntilNs:      option.StopTsNs,
		})
		if err != nil {
			return fmt.Errorf("subscribe: %v", err)
		}

		for {
			resp, listenErr := stream.Recv()
			if listenErr == io.EOF {
				return nil
			}
			if listenErr != nil {
				return listenErr
			}

			if err := processEventFn(resp); err != nil {
				switch option.EventErrorType {
				case TrivialOnError:
					glog.Errorf("process %v: %v", resp, err)
				case FatalOnError:
					glog.Fatalf("process %v: %v", resp, err)
				case RetryForeverOnError:
					util.RetryUntil("followMetaUpdates", func() error {
						return processEventFn(resp)
					}, func(err error) bool {
						glog.Errorf("process %v: %v", resp, err)
						return true
					})
				case DontLogError:
					// pass
				default:
					glog.Errorf("process %v: %v", resp, err)
				}
			}
			option.StartTsNs = resp.TsNs
		}
	}
}

func AddOffsetFunc(processEventFn ProcessMetadataFunc, offsetInterval time.Duration, offsetFunc func(counter int64, offset int64) error) ProcessMetadataFunc {
	var counter int64
	var lastWriteTime time.Time
	return func(resp *filer_pb.SubscribeMetadataResponse) error {
		if err := processEventFn(resp); err != nil {
			return err
		}
		counter++
		if lastWriteTime.Add(offsetInterval).Before(time.Now()) {
			lastWriteTime = time.Now()
			if err := offsetFunc(counter, resp.TsNs); err != nil {
				return err
			}
			counter = 0
		}
		return nil
	}

}
