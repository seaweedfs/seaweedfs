package pb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"io"
	"time"
)

type ProcessMetadataFunc func(resp *filer_pb.SubscribeMetadataResponse) error

func FollowMetadata(filerAddress ServerAddress, grpcDialOption grpc.DialOption, clientName string,
	pathPrefix string, additionalPathPrefixes []string, lastTsNs int64, selfSignature int32,
	processEventFn ProcessMetadataFunc, fatalOnError bool) error {

	err := WithFilerClient(true, filerAddress, grpcDialOption, makeFunc(clientName,
		pathPrefix, additionalPathPrefixes, &lastTsNs, selfSignature, processEventFn, fatalOnError))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %v", err)
	}
	return err
}

func WithFilerClientFollowMetadata(filerClient filer_pb.FilerClient,
	clientName string, pathPrefix string, lastTsNs *int64, selfSignature int32,
	processEventFn ProcessMetadataFunc, fatalOnError bool) error {

	err := filerClient.WithFilerClient(true, makeFunc(clientName,
		pathPrefix, nil, lastTsNs, selfSignature, processEventFn, fatalOnError))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %v", err)
	}

	return nil
}

func makeFunc(clientName string, pathPrefix string, additionalPathPrefixes []string, lastTsNs *int64, selfSignature int32,
	processEventFn ProcessMetadataFunc, fatalOnError bool) func(client filer_pb.SeaweedFilerClient) error {
	return func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:   clientName,
			PathPrefix:   pathPrefix,
			PathPrefixes: additionalPathPrefixes,
			SinceNs:      *lastTsNs,
			Signature:    selfSignature,
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
				if fatalOnError {
					glog.Fatalf("process %v: %v", resp, err)
				} else {
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
			counter = 0
			lastWriteTime = time.Now()
			if err := offsetFunc(counter, resp.TsNs); err != nil {
				return err
			}
		}
		return nil
	}

}
