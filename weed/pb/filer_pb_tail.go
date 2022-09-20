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
)

type ProcessMetadataFunc func(resp *filer_pb.SubscribeMetadataResponse) error

func FollowMetadata(filerAddress ServerAddress, grpcDialOption grpc.DialOption, clientName string, clientId int32, clientEpoch int32,
	pathPrefix string, additionalPathPrefixes []string, lastTsNs int64, untilTsNs int64, selfSignature int32,
	processEventFn ProcessMetadataFunc, eventErrorType EventErrorType) error {

	err := WithFilerClient(true, filerAddress, grpcDialOption, makeSubscribeMetadataFunc(clientName, clientId, clientEpoch, pathPrefix, additionalPathPrefixes, nil, &lastTsNs, untilTsNs, selfSignature, processEventFn, eventErrorType))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %v", err)
	}
	return err
}

func WithFilerClientFollowMetadata(filerClient filer_pb.FilerClient, clientName string, clientId int32, clientEpoch int32, pathPrefix string, directoriesToWatch []string, lastTsNs *int64, untilTsNs int64, selfSignature int32, processEventFn ProcessMetadataFunc, eventErrorType EventErrorType) error {

	err := filerClient.WithFilerClient(true, makeSubscribeMetadataFunc(clientName, clientId, clientEpoch, pathPrefix, nil, directoriesToWatch, lastTsNs, untilTsNs, selfSignature, processEventFn, eventErrorType))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %v", err)
	}

	return nil
}

func makeSubscribeMetadataFunc(clientName string, clientId int32, clientEpoch int32, pathPrefix string, additionalPathPrefixes []string, directoriesToWatch []string, lastTsNs *int64, untilTsNs int64, selfSignature int32, processEventFn ProcessMetadataFunc, eventErrorType EventErrorType) func(client filer_pb.SeaweedFilerClient) error {
	return func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:   clientName,
			PathPrefix:   pathPrefix,
			PathPrefixes: additionalPathPrefixes,
			Directories:  directoriesToWatch,
			SinceNs:      *lastTsNs,
			Signature:    selfSignature,
			ClientId:     clientId,
			ClientEpoch:  clientEpoch,
			UntilNs:      untilTsNs,
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
				switch eventErrorType {
				case TrivialOnError:
					glog.Errorf("process %v: %v", resp, err)
				case FatalOnError:
					glog.Fatalf("process %v: %v", resp, err)
				case RetryForeverOnError:
					util.RetryForever("followMetaUpdates", func() error {
						return processEventFn(resp)
					}, func(err error) bool {
						glog.Errorf("process %v: %v", resp, err)
						return true
					})
				default:
					glog.Errorf("process %v: %v", resp, err)
				}
			}
			*lastTsNs = resp.TsNs
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
