package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
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
	"github.com/seaweedfs/seaweedfs/weed/stats"
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
	// peerWatermarks tracks, per subscribed peer (self included), the newest
	// timestamp received on that peer's stream (event or idle heartbeat).
	// The minimum is a delivery low-watermark: MetaLogBuffer is complete up
	// to it, so a subscriber held at or below it cannot miss a late-merged
	// peer event. An unsignalled peer pins it at zero.
	peerWatermarks map[pb.ServerAddress]int64
	// peerFlushWatermarks tracks each peer's reported flush watermark:
	// everything at or below it is on that peer's disk. The minimum bounds
	// persisted-log reads — beyond it a peer may still land a log file (or
	// chunk) whose events a passed cursor would skip.
	peerFlushWatermarks map[pb.ServerAddress]int64
	// peerRemovedAtNs marks peers the master removed, with the removal time.
	// A removed peer keeps participating in the low-watermarks for a grace
	// period: removal usually means a flap (frozen or partitioned filer)
	// whose unflushed events still exist, and de-accounting it at once would
	// let subscribers advance past them. A re-add clears the mark; a peer
	// gone past the grace is dropped so it cannot pin the low-watermarks.
	peerRemovedAtNs map[pb.ServerAddress]int64
	// lowWatermarkTsNs and lowFlushWatermarkTsNs are the last minima signalled
	// on deliveryAdvanced and flushAdvanced, each closed and replaced when its
	// own minimum rises. Held readers park on the one that bounds them:
	// arriving data cannot release a watermark hold, only a peer reporting
	// the progress that hold waits on. The two are kept apart because a
	// delivery advance - one per event a peer streams - cannot release a read
	// held at the flush watermark, and waking it costs a whole pass.
	lowWatermarkTsNs      int64
	lowFlushWatermarkTsNs int64
	deliveryAdvanced      chan struct{}
	flushAdvanced         chan struct{}
	peerWatermarksLock    sync.Mutex
}

// MetaAggregator only aggregates data "on the fly". The logs are not re-persisted to disk.
// The old data comes from what each LocalMetadata persisted on disk.
func NewMetaAggregator(filer *Filer, self pb.ServerAddress, grpcDialOption grpc.DialOption) *MetaAggregator {
	t := &MetaAggregator{
		filer:               filer,
		self:                self,
		grpcDialOption:      grpcDialOption,
		peerChans:           make(map[pb.ServerAddress]chan struct{}),
		peerWatermarks:      make(map[pb.ServerAddress]int64),
		peerFlushWatermarks: make(map[pb.ServerAddress]int64),
		peerRemovedAtNs:     make(map[pb.ServerAddress]int64),
	}
	// nil notifyFn: aggregated subscribers wake through the buffer's
	// subscriber channels, not a cond.
	t.MetaLogBuffer = log_buffer.NewLogBuffer("aggr", LogFlushInterval, nil, nil, nil)
	return t
}

func (ma *MetaAggregator) OnPeerUpdate(update *master_pb.ClusterNodeUpdate, startFrom time.Time) {
	ma.peerChansLock.Lock()
	defer ma.peerChansLock.Unlock()

	address := pb.ServerAddress(update.Address)
	if update.IsAdd {
		// the peer is already followed, restarting would only lose the events
		// in between
		if _, found := ma.peerChans[address]; found {
			return
		}
		stopChan := make(chan struct{})
		ma.peerChans[address] = stopChan
		// Account for the peer before its stream signals; keep prior values
		// on reconnect.
		ma.initPeerWatermark(address)
		go ma.loopSubscribeToOneFiler(ma.filer, ma.self, address, startFrom, stopChan)
	} else {
		if prevChan, found := ma.peerChans[address]; found {
			close(prevChan)
			delete(ma.peerChans, address)
		}
		// Only mark: dropping the watermarks at once would let subscribers
		// advance past a flapping peer's unflushed events (see peerRemovedAtNs).
		ma.markPeerWatermarkRemoved(address)
	}
}

// peerWatermarkRemovalGrace is how long a removed peer keeps participating
// in the low-watermarks. Matches the subscribe loops' settled horizon, which
// already bounds a stale watermark's influence within it.
const peerWatermarkRemovalGrace = 2 * LogFlushInterval

// initPeerWatermark adds the peer with a zero (unknown) watermark; a re-added
// peer keeps its prior values and stops being considered removed.
func (ma *MetaAggregator) initPeerWatermark(peer pb.ServerAddress) {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	if _, found := ma.peerWatermarks[peer]; !found {
		ma.peerWatermarks[peer] = 0
	}
	if _, found := ma.peerFlushWatermarks[peer]; !found {
		ma.peerFlushWatermarks[peer] = 0
	}
	delete(ma.peerRemovedAtNs, peer)
	ma.noteLowWatermarksLocked()
}

func (ma *MetaAggregator) markPeerWatermarkRemoved(peer pb.ServerAddress) {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	if _, found := ma.peerWatermarks[peer]; !found {
		return
	}
	// First removal time wins: duplicate removals must not refresh the grace.
	if _, marked := ma.peerRemovedAtNs[peer]; !marked {
		ma.peerRemovedAtNs[peer] = time.Now().UnixNano()
	}
}

// dropExpiredRemovedPeersLocked drops peers whose removal outlived the grace.
// Caller must hold peerWatermarksLock.
func (ma *MetaAggregator) dropExpiredRemovedPeersLocked() {
	if len(ma.peerRemovedAtNs) == 0 {
		return
	}
	cutoff := time.Now().UnixNano() - int64(peerWatermarkRemovalGrace)
	for peer, removedAt := range ma.peerRemovedAtNs {
		if removedAt < cutoff {
			delete(ma.peerRemovedAtNs, peer)
			delete(ma.peerWatermarks, peer)
			delete(ma.peerFlushWatermarks, peer)
		}
	}
	// Dropping the peer that pinned a minimum raises it, same as it reporting.
	ma.noteLowWatermarksLocked()
}

// advancePeerWatermark records the peer as received-through tsNs. Monotonic,
// and only for tracked peers: a dropped peer's straggler must not recreate
// its entry and pin the low-watermark.
func (ma *MetaAggregator) advancePeerWatermark(peer pb.ServerAddress, tsNs int64) {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	if cur, found := ma.peerWatermarks[peer]; found && tsNs > cur {
		ma.peerWatermarks[peer] = tsNs
		ma.noteLowWatermarksLocked()
	}
}

// advancePeerFlushWatermark records the peer's reported flush watermark.
// Monotonic, and only for tracked peers (see advancePeerWatermark).
func (ma *MetaAggregator) advancePeerFlushWatermark(peer pb.ServerAddress, tsNs int64) {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	if cur, found := ma.peerFlushWatermarks[peer]; found && tsNs > cur {
		ma.peerFlushWatermarks[peer] = tsNs
		ma.noteLowWatermarksLocked()
	}
}

// PeerLowFlushWatermarkTsNs returns the minimum reported flush watermark
// across all current peers: every peer's events at or below this time are on
// disk. Returns 0 when any peer has not reported yet (completeness unknown).
func (ma *MetaAggregator) PeerLowFlushWatermarkTsNs() int64 {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	ma.dropExpiredRemovedPeersLocked()
	return lowWatermarkOf(ma.peerFlushWatermarks)
}

// PeerLowWatermarkTsNs returns the minimum received-through timestamp across
// all current peers (self included): MetaLogBuffer is complete up to this
// time. Returns 0 when any peer has not signalled yet (or none are tracked),
// i.e. completeness is unknown.
func (ma *MetaAggregator) PeerLowWatermarkTsNs() int64 {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	ma.dropExpiredRemovedPeersLocked()
	return lowWatermarkOf(ma.peerWatermarks)
}

// DeliveryWatermarkAdvancedChan returns a channel closed the next time the
// delivery low-watermark rises, which is what releases an in-memory read held
// at it. PeerLowFlushWatermarkTsNs has FlushWatermarkAdvancedChan. Callers
// must take the channel before reading the watermark they hold at, so a rise
// in between wakes them instead of being missed.
func (ma *MetaAggregator) DeliveryWatermarkAdvancedChan() <-chan struct{} {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	if ma.deliveryAdvanced == nil {
		ma.deliveryAdvanced = make(chan struct{})
	}
	return ma.deliveryAdvanced
}

// FlushWatermarkAdvancedChan returns a channel closed the next time the flush
// low-watermark rises, which is what releases a persisted-log read held at it.
func (ma *MetaAggregator) FlushWatermarkAdvancedChan() <-chan struct{} {
	ma.peerWatermarksLock.Lock()
	defer ma.peerWatermarksLock.Unlock()
	if ma.flushAdvanced == nil {
		ma.flushAdvanced = make(chan struct{})
	}
	return ma.flushAdvanced
}

// noteLowWatermarksLocked recomputes both minima and wakes the readers parked
// on each one that rose. Caller must hold peerWatermarksLock.
func (ma *MetaAggregator) noteLowWatermarksLocked() {
	low, flushLow := lowWatermarkOf(ma.peerWatermarks), lowWatermarkOf(ma.peerFlushWatermarks)
	// Falls are recorded too (a joining peer sits at 0 until it signals), so
	// the climb back out of one is seen as a rise.
	if low > ma.lowWatermarkTsNs {
		ma.deliveryAdvanced = closeWatermarkChan(ma.deliveryAdvanced)
	}
	if flushLow > ma.lowFlushWatermarkTsNs {
		ma.flushAdvanced = closeWatermarkChan(ma.flushAdvanced)
	}
	ma.lowWatermarkTsNs, ma.lowFlushWatermarkTsNs = low, flushLow
}

// closeWatermarkChan wakes everyone parked on ch and clears it, so the next
// caller parks on a fresh one.
func closeWatermarkChan(ch chan struct{}) chan struct{} {
	if ch != nil {
		close(ch)
	}
	return nil
}

// lowWatermarkOf returns the minimum across the tracked peers, or 0 when none
// are tracked: a peer that has not signalled sits at 0 and pins the minimum
// there, which is what makes completeness unknown.
func lowWatermarkOf(watermarks map[pb.ServerAddress]int64) int64 {
	if len(watermarks) == 0 {
		return 0
	}
	var low int64 = math.MaxInt64
	for _, tsNs := range watermarks {
		if tsNs < low {
			low = tsNs
		}
	}
	return low
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

// HasPeer reports whether address is currently a tracked filer peer (or this
// filer's own address). Callers use this to gate operations on known cluster
// members.
func (ma *MetaAggregator) HasPeer(address pb.ServerAddress) bool {
	if address == ma.self || address.Equals(ma.self) {
		return true
	}
	ma.peerChansLock.Lock()
	defer ma.peerChansLock.Unlock()
	for peer := range ma.peerChans {
		if peer == address || peer.Equals(address) {
			return true
		}
	}
	return false
}

func (ma *MetaAggregator) loopSubscribeToOneFiler(f *Filer, self pb.ServerAddress, peer pb.ServerAddress, startFrom time.Time, stopChan chan struct{}) {
	lastTsNs := startFrom.UnixNano()
	for {
		glog.V(0).Infof("loopSubscribeToOneFiler read %s start from %v %d", peer, time.Unix(0, lastTsNs), lastTsNs)
		nextLastTsNs, err := ma.doSubscribeToOneFiler(f, self, peer, lastTsNs, stopChan)

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

func (ma *MetaAggregator) doSubscribeToOneFiler(f *Filer, self pb.ServerAddress, peer pb.ServerAddress, startFrom int64, stopChan <-chan struct{}) (int64, error) {

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
		} else if errors.Is(err, ErrKvNotFound) {
			// No stored offset — this is the first time connecting to this peer.
			// Traverse the peer's full metadata tree so we get pre-existing data.
			// Record time before traversal and subtract a safety margin to
			// account for clock skew between this filer and the peer. Any
			// duplicate events replayed during the overlap are harmless since
			// Replay does upserts. We use wall-clock time (same domain as the
			// metadata stream TsNs) rather than entry Mtime which is a
			// different concept and can be set to arbitrary values.
			preTraverseTime := time.Now()
			glog.V(0).Infof("no previous offset for peer %s, starting full metadata sync", peer)
			if traverseErr := ma.traversePeerMetadata(f, peer); traverseErr != nil {
				return lastTsNs, fmt.Errorf("initial metadata sync from %s: %v", peer, traverseErr)
			}
			lastTsNs = preTraverseTime.Add(-time.Minute).UnixNano()
			if err := ma.updateOffset(f, peer, peerSignature, lastTsNs); err != nil {
				return lastTsNs, fmt.Errorf("save bootstrap offset for peer %s: %w", peer, err)
			}
			glog.V(0).Infof("completed full metadata sync from peer %s, will stream changes from %v", peer, time.Unix(0, lastTsNs))
		} else {
			return lastTsNs, fmt.Errorf("read offset for peer %s: %w", peer, err)
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
			replicateMetadataChange(f.Store, peer, event)
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

	// The stream will deliver everything after lastTsNs, so the aggregated
	// buffer is (still) complete for this peer up to that point.
	ma.advancePeerWatermark(peer, lastTsNs)

	glog.V(0).Infof("subscribing remote %s meta change: %v, clientId:%d", peer, time.Unix(0, lastTsNs), ma.filer.UniqueFilerId)
	err = pb.WithFilerClient(true, 0, peer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Stop the stream promptly on removal; the blocking Recv below would
		// otherwise only notice when the stream errors on its own.
		go func() {
			select {
			case <-stopChan:
				cancel()
			case <-ctx.Done():
			}
		}()
		atomic.AddInt32(&ma.filer.UniqueFilerEpoch, 1)
		// Construct a log file reader that reads chunks via the peer filer's LookupVolume.
		lookupFn := LookupFn(filerClient{client})
		logFileReaderFn := func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
			return NewChunkStreamReaderFromLookup(ctx, lookupFn, chunks), nil
		}

		stream, err := client.SubscribeLocalMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:                   "filer:" + string(self),
			PathPrefix:                   "/",
			SinceNs:                      lastTsNs,
			ClientId:                     ma.filer.UniqueFilerId,
			ClientEpoch:                  atomic.LoadInt32(&ma.filer.UniqueFilerEpoch),
			ClientSupportsBatching:       true,
			ClientSupportsMetadataChunks: true,
			// Idle heartbeats keep a quiet peer from freezing the low-watermark.
			ClientSupportsIdleHeartbeat: true,
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
			ma.advancePeerWatermark(peer, event.TsNs)
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
					return fmt.Errorf("%w from %s: %w", pb.ErrLogFileRead, peer, readErr)
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
			// Batched events, with the envelope's nil guard mirrored. A nested
			// control message still carries watermark state - dropping it
			// would make healthy flush progress look stalled.
			for _, batchedEvent := range resp.Events {
				if batchedEvent.FlushedTsNs > 0 {
					ma.advancePeerFlushWatermark(peer, batchedEvent.FlushedTsNs)
				}
				if batchedEvent.EventNotification == nil {
					if batchedEvent.TsNs > 0 {
						ma.advancePeerWatermark(peer, batchedEvent.TsNs)
					}
					continue
				}
				if err := processOne(batchedEvent); err != nil {
					return err
				}
			}
			// Idle heartbeat: advance only the watermark, not lastTsNs, so a
			// reconnect still resumes from the last real event.
			if resp.EventNotification == nil && len(resp.Events) == 0 && resp.TsNs > 0 {
				ma.advancePeerWatermark(peer, resp.TsNs)
			}
			if resp.FlushedTsNs > 0 {
				ma.advancePeerFlushWatermark(peer, resp.FlushedTsNs)
			}
		}
	})
	return lastTsNs, err
}

// replicateMetadataChange retries transient Replay failures with bounded
// backoff. A failure that outlives the retry budget is counted and logged,
// then skipped: blocking on an event that can never replay would stall every
// later event from this peer, which is worse than one entry staying stale.
func replicateMetadataChange(store FilerStore, peer pb.ServerAddress, event *filer_pb.SubscribeMetadataResponse) {
	err := util.Retry("replicate metadata change from "+string(peer), func() error {
		return Replay(store, event)
	})
	if err == nil {
		return
	}
	stats.FilerMetaAggregatorReplayFailures.WithLabelValues(string(peer)).Inc()
	name := event.GetEventNotification().GetNewEntry().GetName()
	if name == "" {
		name = event.GetEventNotification().GetOldEntry().GetName()
	}
	glog.Errorf("giving up replicating metadata change from %s for %s/%s (ts=%d): %v", peer, event.Directory, name, event.TsNs, err)
}

// traversePeerMetadata does a full BFS traversal of a peer filer's metadata
// and inserts all entries into the local store. This is used when a filer
// connects to a peer for the first time and needs to bootstrap pre-existing data.
func (ma *MetaAggregator) traversePeerMetadata(f *Filer, peer pb.ServerAddress) error {
	return pb.WithFilerClient(true, 0, peer, ma.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.TraverseBfsMetadata(ctx, &filer_pb.TraverseBfsMetadataRequest{
			Directory:        "/",
			ExcludedPrefixes: []string{SystemLogDir},
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
			fullpath := util.Join(resp.Directory, resp.Entry.Name)
			entry := FromPbEntry(resp.Directory, resp.Entry)
			if insertErr := f.Store.InsertEntry(context.Background(), entry); insertErr != nil {
				// Entry may already exist (root dir, or partial previous bootstrap).
				existing, findErr := f.Store.FindEntry(context.Background(), entry.FullPath)
				if findErr != nil {
					return fmt.Errorf("insert entry %s: %w", fullpath, insertErr)
				}
				// Only overwrite if the peer's entry is newer.
				if entry.Attr.Mtime.After(existing.Attr.Mtime) {
					if updateErr := f.Store.UpdateEntry(context.Background(), entry); updateErr != nil {
						return fmt.Errorf("update entry %s: %w", fullpath, updateErr)
					}
				} else {
					glog.V(1).Infof("skip older peer entry %s (peer mtime %v <= local mtime %v)", fullpath, entry.Attr.Mtime, existing.Attr.Mtime)
				}
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
		return 0, fmt.Errorf("readOffset %s : %w", peer, err)
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
