package filer2

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util/log_buffer"
)

type MetaAggregator struct {
	filers         []string
	grpcDialOption grpc.DialOption
	MetaLogBuffer  *log_buffer.LogBuffer
	// notifying clients
	ListenersLock sync.Mutex
	ListenersCond *sync.Cond
}

func NewMetaAggregator(filers []string, grpcDialOption grpc.DialOption) *MetaAggregator {
	t := &MetaAggregator{
		filers:         filers,
		grpcDialOption: grpcDialOption,
	}
	t.ListenersCond = sync.NewCond(&t.ListenersLock)
	t.MetaLogBuffer = log_buffer.NewLogBuffer(time.Minute, nil, func() {
		t.ListenersCond.Broadcast()
	})
	return t
}

func (ma *MetaAggregator) StartLoopSubscribe(lastTsNs int64) {
	for _, filer := range ma.filers {
		go ma.subscribeToOneFiler(filer, lastTsNs)
	}
}

func (ma *MetaAggregator) subscribeToOneFiler(filer string, lastTsNs int64) {

	processEventFn := func(event *filer_pb.SubscribeMetadataResponse) error {
		data, err := proto.Marshal(event)
		if err != nil {
			glog.Errorf("failed to marshal subscribed filer_pb.SubscribeMetadataResponse %+v: %v", event, err)
			return err
		}
		dir := event.Directory
		println("received meta change", dir, "size", len(data))
		ma.MetaLogBuffer.AddToBuffer([]byte(dir), data)
		return nil
	}

	for {
		err := pb.WithFilerClient(filer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			stream, err := client.SubscribeLocalMetadata(context.Background(), &filer_pb.SubscribeMetadataRequest{
				ClientName: "filer",
				PathPrefix: "/",
				SinceNs:    lastTsNs,
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
					return fmt.Errorf("process %v: %v", resp, err)
				}
				lastTsNs = resp.TsNs
			}
		})
		if err != nil {
			glog.V(0).Infof("subscribing remote %s meta change: %v", filer, err)
			time.Sleep(1733 * time.Millisecond)
		}
	}
}
