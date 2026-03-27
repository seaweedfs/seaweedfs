package pb

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
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
	// LogFileReaderFn, when non-nil, enables metadata chunks mode:
	// the server sends log file chunk fids instead of streaming events,
	// and the client reads directly from volume servers.
	LogFileReaderFn        LogFileReaderFn
}

type ProcessMetadataFunc func(resp *filer_pb.SubscribeMetadataResponse) error

func FollowMetadata(filerAddress ServerAddress, grpcDialOption grpc.DialOption, option *MetadataFollowOption, processEventFn ProcessMetadataFunc) error {

	err := WithFilerClient(true, option.SelfSignature, filerAddress, grpcDialOption, makeSubscribeMetadataFunc(option, processEventFn))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %w", err)
	}
	return err
}

func WithFilerClientFollowMetadata(filerClient filer_pb.FilerClient, option *MetadataFollowOption, processEventFn ProcessMetadataFunc) error {

	err := filerClient.WithFilerClient(true, makeSubscribeMetadataFunc(option, processEventFn))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %w", err)
	}

	return nil
}

func makeSubscribeMetadataFunc(option *MetadataFollowOption, processEventFn ProcessMetadataFunc) func(client filer_pb.SeaweedFilerClient) error {
	return func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:                    option.ClientName,
			PathPrefix:                    option.PathPrefix,
			PathPrefixes:                  option.AdditionalPathPrefixes,
			Directories:                   option.DirectoriesToWatch,
			SinceNs:                       option.StartTsNs,
			Signature:                     option.SelfSignature,
			ClientId:                      option.ClientId,
			ClientEpoch:                   option.ClientEpoch,
			UntilNs:                       option.StopTsNs,
			ClientSupportsBatching:        true,
			ClientSupportsMetadataChunks:  option.LogFileReaderFn != nil,
		})
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}

		handleErr := func(resp *filer_pb.SubscribeMetadataResponse, err error) {
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

		var pendingRefs []*filer_pb.LogFileChunkRef

		for {
			resp, listenErr := stream.Recv()
			if listenErr == io.EOF {
				return nil
			}
			if listenErr != nil {
				return listenErr
			}

			// Accumulate log file chunk references (metadata chunks mode)
			if len(resp.LogFileRefs) > 0 {
				pendingRefs = append(pendingRefs, resp.LogFileRefs...)
				continue
			}

			// Process accumulated refs before handling normal events (transition point)
			if len(pendingRefs) > 0 && option.LogFileReaderFn != nil {
				lastTs, readErr := ReadLogFileRefs(pendingRefs, option.LogFileReaderFn,
					option.StartTsNs, option.StopTsNs,
					PathFilter{
						PathPrefix:             option.PathPrefix,
						AdditionalPathPrefixes: option.AdditionalPathPrefixes,
						DirectoriesToWatch:     option.DirectoriesToWatch,
					},
					processEventFn)
				if readErr != nil {
					return fmt.Errorf("read log file refs: %w", readErr)
				}
				if lastTs > 0 {
					option.StartTsNs = lastTs
				}
				pendingRefs = nil
			}

			// Process the first event (always present in top-level fields)
			if resp.EventNotification != nil {
				if err := processEventFn(resp); err != nil {
					handleErr(resp, err)
				}
				option.StartTsNs = resp.TsNs
			}

			// Process any additional batched events
			for _, batchedEvent := range resp.Events {
				if err := processEventFn(batchedEvent); err != nil {
					handleErr(batchedEvent, err)
				}
				option.StartTsNs = batchedEvent.TsNs
			}
		}
	}
}

func AddOffsetFunc(processEventFn ProcessMetadataFunc, offsetInterval time.Duration, offsetFunc func(counter int64, offset int64) error) ProcessMetadataFunc {
	var counter int64
	var lastWriteTime = time.Now()
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
