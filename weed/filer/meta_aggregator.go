package filer

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

type MetaAggregator struct {
	filer          *Filer
	self           pb.ServerAddress
	isLeader       bool
	grpcDialOption grpc.DialOption
	MetaLogBuffer  *log_buffer.LogBuffer
	peerChans      map[pb.ServerAddress]chan struct{}
	peerChansLock  sync.Mutex
	// notifying clients
	ListenersLock sync.Mutex
	ListenersCond *sync.Cond
}

// MetaAggregator only aggregates data "on the fly". The logs are not re-persisted to disk.
// The old data comes from what each LocalMetadata persisted on disk.
func NewMetaAggregator(filer *Filer, self pb.ServerAddress, grpcDialOption grpc.DialOption) *MetaAggregator {
	t := &MetaAggregator{
		filer:          filer,
		self:           self,
		grpcDialOption: grpcDialOption,
		peerChans:      make(map[pb.ServerAddress]chan struct{}),
	}
	t.ListenersCond = sync.NewCond(&t.ListenersLock)
	t.MetaLogBuffer = log_buffer.NewLogBuffer("aggr", LogFlushInterval, nil, nil, func() {
		t.ListenersCond.Broadcast()
	})
	return t
}

func (ma *MetaAggregator) OnPeerUpdate(update *master_pb.ClusterNodeUpdate, startFrom time.Time) {
	ma.peerChansLock.Lock()
	defer ma.peerChansLock.Unlock()

	address := pb.ServerAddress(update.Address)
	if update.IsAdd {
		// cancel previous subscription if any
		if prevChan, found := ma.peerChans[address]; found {
			close(prevChan)
		}
		stopChan := make(chan struct{})
		ma.peerChans[address] = stopChan
		go ma.loopSubscribeToOneFiler(ma.filer, ma.self, address, startFrom, stopChan)
	} else {
		if prevChan, found := ma.peerChans[address]; found {
			close(prevChan)
			delete(ma.peerChans, address)
		}
	}
}

func (ma *MetaAggregator) loopSubscribeToOneFiler(f *Filer, self pb.ServerAddress, peer pb.ServerAddress, startFrom time.Time, stopChan chan struct{}) {
	lastTsNs := startFrom.UnixNano()
	for {
		glog.V(0).Infof("loopSubscribeToOneFiler read %s start from %v %d", peer, time.Unix(0, lastTsNs), lastTsNs)
		nextLastTsNs, err := ma.doSubscribeToOneFiler(f, self, peer, lastTsNs)

		// check stopChan to see if we should stop
		select {
		case <-stopChan:
			glog.V(0).Infof("stop subscribing peer %s meta change", peer)
			return
		default:
		}

		if err != nil {
			errLvl := glog.Level(0)
			if strings.Contains(err.Error(), "duplicated local subscription detected") {
				errLvl = glog.Level(4)
			}
			glog.V(errLvl).Infof("subscribing remote %s meta change: %v", peer, err)
		}
		if lastTsNs < nextLastTsNs {
			lastTsNs = nextLastTsNs
		}
		time.Sleep(1733 * time.Millisecond)
	}
}

func (ma *MetaAggregator) doSubscribeToOneFiler(f *Filer, self pb.ServerAddress, peer pb.ServerAddress, startFrom int64) (int64, error) {

	/*
		Each filer reads the "filer.store.id", which is the store's signature when filer starts.

		When reading from other filers' local meta changes:
		* if the received change does not contain signature from self, apply the change to current filer store.

		Upon connecting to other filers, need to remember their signature and their offsets.

	*/

	var maybeReplicateMetadataChange func(*filer_pb.SubscribeMetadataResponse)
	lastPersistTime := time.Now()
	lastTsNs := startFrom

	peerSignature, err := ma.readFilerStoreSignature(peer)
	if err != nil {
		return lastTsNs, fmt.Errorf("connecting to peer filer %s: %v", peer, err)
	}

	// when filer store is not shared by multiple filers
	if peerSignature != f.Signature {
		if prevTsNs, err := ma.readOffset(f, peer, peerSignature); err == nil {
			lastTsNs = prevTsNs
			defer func(prevTsNs int64) {
				if lastTsNs != prevTsNs && lastTsNs != lastPersistTime.UnixNano() {
					if err := ma.updateOffset(f, peer, peerSignature, lastTsNs); err == nil {
						glog.V(0).Infof("last sync time with %s at %v (%d)", peer, time.Unix(0, lastTsNs), lastTsNs)
					} else {
						glog.Errorf("failed to save last sync time with %s at %v (%d)", peer, time.Unix(0, lastTsNs), lastTsNs)
					}
				}
			}(prevTsNs)
		}

		glog.V(0).Infof("follow peer: %v, last %v (%d)", peer, time.Unix(0, lastTsNs), lastTsNs)
		var counter int64
		var synced bool
		maybeReplicateMetadataChange = func(event *filer_pb.SubscribeMetadataResponse) {
			if err := Replay(f.Store, event); err != nil {
				glog.Errorf("failed to reply metadata change from %v: %v", peer, err)
				return
			}
			counter++
			if lastPersistTime.Add(time.Minute).Before(time.Now()) {
				if err := ma.updateOffset(f, peer, peerSignature, event.TsNs); err == nil {
					if event.TsNs < time.Now().Add(-2*time.Minute).UnixNano() {
						glog.V(0).Infof("sync with %s progressed to: %v %0.2f/sec", peer, time.Unix(0, event.TsNs), float64(counter)/60.0)
					} else if !synced {
						synced = true
						glog.V(0).Infof("synced with %s", peer)
					}
					lastPersistTime = time.Now()
					counter = 0
				} else {
					glog.V(0).Infof("failed to update offset for %v: %v", peer, err)
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
		ma.MetaLogBuffer.AddDataToBuffer([]byte(dir), data, event.TsNs)
		if maybeReplicateMetadataChange != nil {
			maybeReplicateMetadataChange(event)
		}
		return nil
	}

	glog.V(0).Infof("subscribing remote %s meta change: %v, clientId:%d", peer, time.Unix(0, lastTsNs), ma.filer.UniqueFilerId)
	err = pb.WithFilerClient(true, 0, peer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		atomic.AddInt32(&ma.filer.UniqueFilerEpoch, 1)
		stream, err := client.SubscribeLocalMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:  "filer:" + string(self),
			PathPrefix:  "/",
			SinceNs:     lastTsNs,
			ClientId:    ma.filer.UniqueFilerId,
			ClientEpoch: atomic.LoadInt32(&ma.filer.UniqueFilerEpoch),
		})
		if err != nil {
			glog.V(0).Infof("SubscribeLocalMetadata %v: %v", peer, err)
			return fmt.Errorf("subscribe: %v", err)
		}

		for {
			resp, listenErr := stream.Recv()
			if listenErr == io.EOF {
				return nil
			}
			if listenErr != nil {
				glog.V(0).Infof("SubscribeLocalMetadata stream %v: %v", peer, listenErr)
				return listenErr
			}

			if err := processEventFn(resp); err != nil {
				glog.V(0).Infof("SubscribeLocalMetadata process %v: %v", resp, err)
				return fmt.Errorf("process %v: %v", resp, err)
			}

			f.onMetadataChangeEvent(resp)
			lastTsNs = resp.TsNs
		}
	})
	return lastTsNs, err
}

func (ma *MetaAggregator) readFilerStoreSignature(peer pb.ServerAddress) (sig int32, err error) {
	err = pb.WithFilerClient(false, 0, peer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		sig = resp.Signature
		return nil
	})
	return
}

const (
	MetaOffsetPrefix = "Meta"
)

func GetPeerMetaOffsetKey(peerSignature int32) []byte {
	key := []byte(MetaOffsetPrefix + "xxxx")
	util.Uint32toBytes(key[len(MetaOffsetPrefix):], uint32(peerSignature))
	return key
}

func (ma *MetaAggregator) readOffset(f *Filer, peer pb.ServerAddress, peerSignature int32) (lastTsNs int64, err error) {

	key := GetPeerMetaOffsetKey(peerSignature)

	value, err := f.Store.KvGet(context.Background(), key)

	if err != nil {
		return 0, fmt.Errorf("readOffset %s : %v", peer, err)
	}

	lastTsNs = int64(util.BytesToUint64(value))

	glog.V(0).Infof("readOffset %s : %d", peer, lastTsNs)

	return
}

func (ma *MetaAggregator) updateOffset(f *Filer, peer pb.ServerAddress, peerSignature int32, lastTsNs int64) (err error) {

	key := GetPeerMetaOffsetKey(peerSignature)

	value := make([]byte, 8)
	util.Uint64toBytes(value, uint64(lastTsNs))

	err = f.Store.KvPut(context.Background(), key, value)

	if err != nil {
		return fmt.Errorf("updateOffset %s : %v", peer, err)
	}

	glog.V(4).Infof("updateOffset %s : %d", peer, lastTsNs)

	return
}
