package filer

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

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
	ListenersLock  sync.Mutex
	ListenersWaits int64 // Atomic counter
	ListenersCond  *sync.Cond
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
		if atomic.LoadInt64(&t.ListenersWaits) > 0 {
			t.ListenersCond.Broadcast()
		}
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

func (ma *MetaAggregator) HasRemotePeers() bool {
	ma.peerChansLock.Lock()
	defer ma.peerChansLock.Unlock()

	for address := range ma.peerChans {
		if address != ma.self {
			return true
		}
	}
	return false
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
		} else {
			// No stored offset — this is the first time connecting to this peer.
			// Traverse the peer's full metadata tree so we get pre-existing data,
			// then start streaming changes from the traversal start time.
			traverseStart := time.Now()
			glog.V(0).Infof("no previous offset for peer %s, starting full metadata sync", peer)
			if traverseErr := ma.traversePeerMetadata(f, peer); traverseErr != nil {
				return lastTsNs, fmt.Errorf("initial metadata sync from %s: %v", peer, traverseErr)
			}
			lastTsNs = traverseStart.UnixNano()
			glog.V(0).Infof("completed full metadata sync from peer %s, will stream changes from %v", peer, traverseStart)
		}
		defer func(prevTsNs int64) {
			if lastTsNs != prevTsNs && lastTsNs != lastPersistTime.UnixNano() {
				if err := ma.updateOffset(f, peer, peerSignature, lastTsNs); err == nil {
					glog.V(0).Infof("last sync time with %s at %v (%d)", peer, time.Unix(0, lastTsNs), lastTsNs)
				} else {
					glog.Errorf("failed to save last sync time with %s at %v (%d)", peer, time.Unix(0, lastTsNs), lastTsNs)
				}
			}
		}(lastTsNs)

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
		if err := ma.MetaLogBuffer.AddDataToBuffer([]byte(dir), data, event.TsNs); err != nil {
			glog.Errorf("failed to add data to log buffer for %s: %v", dir, err)
			return err
		}
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
		// Construct a log file reader that reads chunks via the peer filer's LookupVolume.
		lookupFn := LookupFn(filerClient{client})
		logFileReaderFn := func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
			return NewChunkStreamReaderFromLookup(ctx, lookupFn, chunks), nil
		}

		stream, err := client.SubscribeLocalMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:                    "filer:" + string(self),
			PathPrefix:                    "/",
			SinceNs:                       lastTsNs,
			ClientId:                      ma.filer.UniqueFilerId,
			ClientEpoch:                   atomic.LoadInt32(&ma.filer.UniqueFilerEpoch),
			ClientSupportsBatching:        true,
			ClientSupportsMetadataChunks:  true,
		})
		if err != nil {
			glog.V(0).Infof("SubscribeLocalMetadata %v: %v", peer, err)
			return fmt.Errorf("subscribe: %w", err)
		}

		processOne := func(event *filer_pb.SubscribeMetadataResponse) error {
			if err := processEventFn(event); err != nil {
				glog.V(0).Infof("SubscribeLocalMetadata process %v: %v", event, err)
				return fmt.Errorf("process %v: %w", event, err)
			}
			f.onMetadataChangeEvent(event)
			lastTsNs = event.TsNs
			return nil
		}

		var pendingRefs []*filer_pb.LogFileChunkRef

		for {
			resp, listenErr := stream.Recv()
			if listenErr == io.EOF {
				return nil
			}
			if listenErr != nil {
				glog.V(0).Infof("SubscribeLocalMetadata stream %v: %v", peer, listenErr)
				return listenErr
			}

			// Accumulate log file chunk references
			if len(resp.LogFileRefs) > 0 {
				pendingRefs = append(pendingRefs, resp.LogFileRefs...)
				continue
			}

			// Process accumulated refs (transition from disk to in-memory)
			if len(pendingRefs) > 0 {
				lastTs, readErr := pb.ReadLogFileRefs(pendingRefs, logFileReaderFn,
					lastTsNs, 0, pb.PathFilter{PathPrefix: "/"},
					func(event *filer_pb.SubscribeMetadataResponse) error {
						return processOne(event)
					})
				if readErr != nil {
					return fmt.Errorf("read log file refs from %s: %w", peer, readErr)
				}
				if lastTs > 0 {
					lastTsNs = lastTs
				}
				pendingRefs = nil
			}

			if resp.EventNotification != nil {
				if err := processOne(resp); err != nil {
					return err
				}
			}
			// Process any additional batched events
			for _, batchedEvent := range resp.Events {
				if err := processOne(batchedEvent); err != nil {
					return err
				}
			}
		}
	})
	return lastTsNs, err
}

// traversePeerMetadata does a full BFS traversal of a peer filer's metadata
// and inserts all entries into the local store. This is used when a filer
// connects to a peer for the first time and needs to bootstrap pre-existing data.
func (ma *MetaAggregator) traversePeerMetadata(f *Filer, peer pb.ServerAddress) error {
	return pb.WithFilerClient(true, 0, peer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.TraverseBfsMetadata(ctx, &filer_pb.TraverseBfsMetadataRequest{
			Directory: "/",
		})
		if err != nil {
			return fmt.Errorf("traverse bfs metadata: %w", err)
		}
		var count int64
		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return fmt.Errorf("traverse bfs metadata recv: %w", recvErr)
			}
			if resp.Entry == nil {
				continue
			}
			// skip filer internal meta logs
			fullpath := util.Join(resp.Directory, resp.Entry.Name)
			if strings.HasPrefix(fullpath, SystemLogDir) {
				continue
			}
			entry := FromPbEntry(resp.Directory, resp.Entry)
			if err := f.Store.InsertEntry(context.Background(), entry); err != nil {
				return fmt.Errorf("insert entry %s: %w", fullpath, err)
			}
			count++
			if count%10000 == 0 {
				glog.V(0).Infof("synced %d entries from peer %s", count, peer)
			}
		}
		glog.V(0).Infof("synced %d entries total from peer %s", count, peer)
		return nil
	})
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

// filerClient adapts a SeaweedFilerClient to the FilerClient interface
// for use with LookupFn. Used by MetaAggregator to resolve volume IDs
// on peer filers.
type filerClient struct {
	client filer_pb.SeaweedFilerClient
}

func (fc filerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(fc.client)
}

func (fc filerClient) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (fc filerClient) GetDataCenter() string {
	return ""
}
