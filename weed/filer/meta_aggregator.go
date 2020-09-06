package filer

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
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

// MetaAggregator only aggregates data "on the fly". The logs are not re-persisted to disk.
// The old data comes from what each LocalMetadata persisted on disk.
func NewMetaAggregator(filers []string, grpcDialOption grpc.DialOption) *MetaAggregator {
	t := &MetaAggregator{
		filers:         filers,
		grpcDialOption: grpcDialOption,
	}
	t.ListenersCond = sync.NewCond(&t.ListenersLock)
	t.MetaLogBuffer = log_buffer.NewLogBuffer(LogFlushInterval, nil, func() {
		t.ListenersCond.Broadcast()
	})
	return t
}

func (ma *MetaAggregator) StartLoopSubscribe(f *Filer, self string) {
	for _, filer := range ma.filers {
		go ma.subscribeToOneFiler(f, self, filer)
	}
}

func (ma *MetaAggregator) subscribeToOneFiler(f *Filer, self string, filer string) {

	/*
		Each filer reads the "filer.store.id", which is the store's signature when filer starts.

		When reading from other filers' local meta changes:
		* if the received change does not contain signature from self, apply the change to current filer store.

		Upon connecting to other filers, need to remember their signature and their offsets.

	*/

	var maybeReplicateMetadataChange func(*filer_pb.SubscribeMetadataResponse)
	lastPersistTime := time.Now()
	lastTsNs := time.Now().Add(-LogFlushInterval).UnixNano()

	isSameFilerStore, err := ma.isSameFilerStore(f, filer)
	for err != nil {
		glog.V(0).Infof("connecting to peer filer %s: %v", filer, err)
		time.Sleep(1357 * time.Millisecond)
		isSameFilerStore, err = ma.isSameFilerStore(f, filer)
	}

	if !isSameFilerStore {
		if prevTsNs, err := ma.readOffset(f, filer); err == nil {
			lastTsNs = prevTsNs
		}

		glog.V(0).Infof("follow filer: %v, last %v (%d)", filer, time.Unix(0, lastTsNs), lastTsNs)
		maybeReplicateMetadataChange = func(event *filer_pb.SubscribeMetadataResponse) {
			if err := Replay(f.Store.ActualStore, event); err != nil {
				glog.Errorf("failed to reply metadata change from %v: %v", filer, err)
				return
			}
			if lastPersistTime.Add(time.Minute).Before(time.Now()) {
				if err := ma.updateOffset(f, filer, event.TsNs); err == nil {
					if event.TsNs < time.Now().Add(-2*time.Minute).UnixNano() {
						glog.V(0).Infof("sync with %s progressed to: %v", filer, time.Unix(0, event.TsNs).UTC())
					}
					lastPersistTime = time.Now()
				} else {
					glog.V(0).Infof("failed to update offset for %v: %v", filer, err)
				}
			}
		}
	}

	processEventFn := func(event *filer_pb.SubscribeMetadataResponse) error {
		data, err := proto.Marshal(event)
		if err != nil {
			glog.Errorf("failed to marshal subscribed filer_pb.SubscribeMetadataResponse %+v: %v", event, err)
			return err
		}
		dir := event.Directory
		// println("received meta change", dir, "size", len(data))
		ma.MetaLogBuffer.AddToBuffer([]byte(dir), data, event.TsNs)
		if maybeReplicateMetadataChange != nil {
			maybeReplicateMetadataChange(event)
		}
		return nil
	}

	for {
		err := pb.WithFilerClient(filer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			stream, err := client.SubscribeLocalMetadata(context.Background(), &filer_pb.SubscribeMetadataRequest{
				ClientName: "filer:" + self,
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

func (ma *MetaAggregator) isSameFilerStore(f *Filer, peer string) (isSame bool, err error) {
	err = pb.WithFilerClient(peer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		isSame = f.Signature == resp.Signature
		return nil
	})
	return
}

func (ma *MetaAggregator) readOffset(f *Filer, peer string) (lastTsNs int64, err error) {

	value, err := f.Store.KvGet(context.Background(), []byte("meta"+peer))

	if err == ErrKvNotFound {
		glog.Warningf("readOffset %s not found", peer)
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("readOffset %s : %v", peer, err)
	}

	lastTsNs = int64(util.BytesToUint64(value))

	glog.V(0).Infof("readOffset %s : %d", peer, lastTsNs)

	return
}

func (ma *MetaAggregator) updateOffset(f *Filer, peer string, lastTsNs int64) (err error) {

	value := make([]byte, 8)
	util.Uint64toBytes(value, uint64(lastTsNs))

	err = f.Store.KvPut(context.Background(), []byte("meta"+peer), value)

	if err != nil {
		return fmt.Errorf("updateOffset %s : %v", peer, err)
	}

	glog.V(4).Infof("updateOffset %s : %d", peer, lastTsNs)

	return
}
