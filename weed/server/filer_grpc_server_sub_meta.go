package weed_server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

const (
	// unflushedGapRetryInterval caps the wait of a subscriber parked on a recent
	// (possibly-unflushed) gap, in case the flush notification is missed.
	unflushedGapRetryInterval = 2 * time.Second

	// gapStallWarnInterval paces the warning for a subscriber that stays parked.
	gapStallWarnInterval = time.Minute

	// maxGapStall bounds how long a subscriber waits for a gap to become
	// readable before the stream is failed. Waiting is productive while the
	// window that holds the gap is still queued for flush, but a peer that never
	// comes back - or a peer whose filer store this filer cannot read at all -
	// makes it permanent, and a subscriber that silently stops delivering is as
	// bad as one that silently skips. Fail loudly instead and let the client
	// decide; a reconnect that hits the same wall reports it again.
	maxGapStall = 15 * time.Minute

	// MaxUnsyncedEvents send empty notification with timestamp when certain amount of events have been filtered
	MaxUnsyncedEvents = 1e3

	// idleHeartbeatInterval bounds how often a caught-up subscriber that asked
	// for idle heartbeats is reminded that the source is alive and has nothing
	// newer. It keeps freshness signals such as filer.sync's sync_offset metric
	// from looking stuck during read-only periods on the source.
	idleHeartbeatInterval = 5 * time.Second
)

// metadataStreamSender is satisfied by both gRPC stream types and pipelinedSender.
type metadataStreamSender interface {
	Send(*filer_pb.SubscribeMetadataResponse) error
}

const (
	// batchBehindThreshold: when an event's timestamp is older than this
	// relative to wall clock, the sender switches to batch mode for throughput.
	// When events are closer to current time, they are sent one-by-one for
	// low latency.
	batchBehindThreshold = 2 * time.Minute
	maxBatchSize         = 256
)

// pipelinedSender decouples event reading from gRPC delivery by buffering
// messages in a channel. A dedicated goroutine handles stream.Send(), allowing
// the reader to continue reading ahead without waiting for the client to
// acknowledge each event.
//
// When the client declares support for batching AND events are far behind
// current time (backlog catch-up), multiple events are packed into a single
// stream.Send() using the Events field. Otherwise events are sent one-by-one.
type pipelinedSender struct {
	sendCh   chan *filer_pb.SubscribeMetadataResponse
	errCh    chan error
	done     chan struct{}
	canBatch bool // true only if client set ClientSupportsBatching
}

func newPipelinedSender(stream metadataStreamSender, bufSize int, clientSupportsBatching bool) *pipelinedSender {
	s := &pipelinedSender{
		sendCh:   make(chan *filer_pb.SubscribeMetadataResponse, bufSize),
		errCh:    make(chan error, 1),
		done:     make(chan struct{}),
		canBatch: clientSupportsBatching,
	}
	go s.sendLoop(stream)
	return s
}

func (s *pipelinedSender) sendLoop(stream metadataStreamSender) {
	defer close(s.done)
	for msg := range s.sendCh {
		// LogFileRefs messages are unbatchable: the client recognizes them by
		// the top-level field and skips the rest of the response, so a refs
		// envelope would drop its Events tail and refs inside Events would be
		// applied as an (empty) event. Their TsNs is 0, which the batch
		// heuristic would misread as far behind. Always send them solo.
		shouldBatch := s.canBatch && len(msg.LogFileRefs) == 0 &&
			time.Now().UnixNano()-msg.TsNs > int64(batchBehindThreshold)

		if !shouldBatch {
			// Real-time: send immediately for low latency
			if err := stream.Send(msg); err != nil {
				s.reportErr(err)
				return
			}
			continue
		}

		// Backlog: batch multiple events into one Send for throughput.
		// The first event goes in the top-level fields; additional events
		// go in the Events slice. Old clients ignore the Events field.
		batch := make([]*filer_pb.SubscribeMetadataResponse, 0, maxBatchSize)
		batch = append(batch, msg)
		var trailingRefs *filer_pb.SubscribeMetadataResponse
	drain:
		for len(batch) < maxBatchSize {
			select {
			case next, ok := <-s.sendCh:
				if !ok {
					break drain
				}
				if len(next.LogFileRefs) > 0 {
					// already consumed; send it solo right after the batch
					trailingRefs = next
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}

		var toSend *filer_pb.SubscribeMetadataResponse
		if len(batch) == 1 {
			toSend = batch[0]
		} else {
			// Pack batch: first event is the envelope, rest go in Events
			toSend = batch[0]
			toSend.Events = batch[1:]
		}
		if err := stream.Send(toSend); err != nil {
			s.reportErr(err)
			return
		}
		if toSend.Events != nil {
			toSend.Events = nil
		}
		if trailingRefs != nil {
			if err := stream.Send(trailingRefs); err != nil {
				s.reportErr(err)
				return
			}
		}
	}
}

func (s *pipelinedSender) reportErr(err error) {
	select {
	case s.errCh <- err:
	default:
	}
	// Don't drain sendCh here — Send() detects the exit via <-s.done
	// and the deferred close(s.done) in sendLoop will fire after this returns.
}

func (s *pipelinedSender) Send(msg *filer_pb.SubscribeMetadataResponse) error {
	select {
	case s.sendCh <- msg:
		return nil
	case err := <-s.errCh:
		return err
	case <-s.done:
		// Sender goroutine exited (stream error or shutdown).
		select {
		case err := <-s.errCh:
			return err
		default:
			return fmt.Errorf("pipelined sender closed")
		}
	}
}

func (s *pipelinedSender) Close() error {
	close(s.sendCh)
	<-s.done
	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}

// reportUnprovenAggregatedCrossing records the one gap the aggregated stream
// cannot close by itself.
//
// The eviction watermark is a property of the merged ring, but the disk it is
// checked against is the union of every peer's own log, and each peer flushes
// on its own schedule. A read that advances the cursor from below the watermark
// to above it may have done so entirely on a peer that is already ahead, while
// a lagging peer still holds unflushed events inside the range just skipped;
// once it flushes them they sit behind the cursor and are never delivered.
//
// Nothing available locally distinguishes that from the ordinary case where
// every peer had in fact persisted the range -- the aggregator tracks peers by
// address while log files carry a random per-filer id, so "has this peer
// flushed through T" cannot be answered here. Deciding it needs the source
// filer's own flush watermark carried on the subscribe stream. Until then the
// crossing is counted and logged rather than passed over in silence.
func reportUnprovenAggregatedCrossing(cursorBeforeTsNs, cursorAfterTsNs, evictedTsNs int64, clientName, pathPrefix string) {
	if evictedTsNs == 0 || cursorBeforeTsNs >= evictedTsNs || cursorAfterTsNs < evictedTsNs {
		return
	}
	stats.FilerSubscribeUnprovenGapCrossings.Inc()
	glog.Warningf("aggregated subscriber %s %s crossed an evicted range (%v..%v] on peer disk reads; a peer that flushes into it later will not be re-read",
		clientName, pathPrefix, time.Unix(0, cursorBeforeTsNs), time.Unix(0, evictedTsNs))
}

// diskReadAdvanced reports whether a persisted read moved the subscriber on.
// A chunk-ref read reports the minute-level name of the last file it shipped,
// clamped so it never rewinds, so it comes back non-zero even when it names the
// position that was already current. Treating that as progress clears the stall
// timer, and a subscriber parked on a gap it re-ships the same refs for would
// reset the timer every retry and never reach the stall bound.
func diskReadAdvanced(processedTsNs int64, cursor log_buffer.MessagePosition) bool {
	return processedTsNs != 0 && processedTsNs > cursor.Time.UnixNano()
}

// gapResumeCursorOffset is the sentinel offset a gap resume carries. It has to
// be one ReadFromBuffer will serve: that read only falls through to memory for
// a cursor below the in-memory window when the offset is a sentinel, and hands
// back ResumeFromDiskError for every positive one. A resume the memory read
// then refuses would bounce straight back to the gap resolver, which sees no
// progress and parks a subscriber whose data is sitting in the ring.
const gapResumeCursorOffset = -2

// memoryHoldsGap reports whether the retained ring still holds every entry after
// the cursor, i.e. nothing after it was evicted.
//
// Equality with the watermark counts, whatever the cursor's own inclusivity.
// The evicted window ends on that timestamp and the retained windows start
// strictly after it, so memory holds nothing there; and the persisted reader
// skips ts <= its start position, so no disk read at this cursor can produce
// that entry either, however long the subscriber waits for a flush. Refusing
// the gap here buys nothing and never ends.
func memoryHoldsGap(currentTsNs, lastEvictedTsNs int64) bool {
	if lastEvictedTsNs == 0 {
		return true // nothing was ever dropped from the ring
	}
	return currentTsNs >= lastEvictedTsNs
}

// resolveAggregatedGapResume decides whether an aggregated-stream subscriber may
// skip a gap its disk read found empty. The aggregated ring never flushes (peers
// persist their own local logs), so that miss proves nothing and the eviction
// watermark is the only emptiness proof: below it entries were dropped and only
// a peer's flush can supply them.
func resolveAggregatedGapResume(currentTsNs, earliestMemTsNs, lastEvictedTsNs int64) (advanceToTsNs int64, advance bool) {
	// No in-memory data (zero time → negative UnixNano), or memory not ahead of us.
	if earliestMemTsNs <= 0 || earliestMemTsNs <= currentTsNs {
		return 0, false
	}
	if !memoryHoldsGap(currentTsNs, lastEvictedTsNs) {
		return 0, false
	}
	// Resume just below earliest, not at it. A sealed window holding a single
	// entry has startTime == stopTime == earliest, and the sealed-buffer lookup
	// only enters a window whose stopTime is strictly after the cursor, so a
	// cursor sitting exactly on earliest skips that window entirely and loses
	// its sole event. One nanosecond lower takes the startTime.After branch and
	// returns the whole window.
	target := earliestMemTsNs - 1
	if target <= currentTsNs {
		return 0, false
	}
	return target, true
}

// gapStallReporter makes a parked subscriber visible: a flush that never lands
// stalls the stream for good, and filer.sync and mount followers just stop
// advancing with no error on either side.
//
// The gauge counts parked subscribers per scope. It deliberately carries no
// per-client label: clientName embeds the ephemeral source port (a series per
// reconnect), and the client-supplied name is not unique either - every mount
// registers as "mount" - so same-named streams would clobber and delete each
// other's series. A count needs no identity and no cleanup; the logs carry the
// client details.
type gapStallReporter struct {
	scope      string
	clientName string
	pathPrefix string
	since      time.Time
	lastWarnAt time.Time
}

func (r *gapStallReporter) gauge() prometheus.Gauge {
	return stats.FilerSubscribeGapStalledGauge.WithLabelValues(r.scope)
}

// stalledFor reports how long this subscriber has been parked, zero if it is not.
func (r *gapStallReporter) stalledFor() time.Duration {
	if r.since.IsZero() {
		return 0
	}
	return time.Since(r.since)
}

// park records that the subscriber is waiting on a gap. It stays quiet until
// the stall has lasted gapStallWarnInterval: during a catch-up burst a
// subscriber parks and resumes every couple of seconds, and a warning per
// cycle would bury the long-stall warnings this reporter exists to surface.
func (r *gapStallReporter) park(cursor time.Time, detail string) {
	now := time.Now()
	if r.since.IsZero() {
		r.since = now
		r.gauge().Inc()
	}
	if now.Sub(r.since) < gapStallWarnInterval {
		return
	}
	if !r.lastWarnAt.IsZero() && now.Sub(r.lastWarnAt) < gapStallWarnInterval {
		return
	}
	r.lastWarnAt = now
	glog.Warningf("%s subscriber %s %s parked %v at %v: %s", r.scope, r.clientName, r.pathPrefix,
		now.Sub(r.since).Truncate(time.Second), cursor, detail)
}

// resumed marks the gap cleared. Only a stall park() had already warned about
// is worth announcing.
func (r *gapStallReporter) resumed() {
	if r.since.IsZero() {
		return
	}
	if !r.lastWarnAt.IsZero() {
		glog.Warningf("%s subscriber %s %s resumed after %v parked", r.scope, r.clientName, r.pathPrefix,
			time.Since(r.since).Truncate(time.Second))
	}
	r.since, r.lastWarnAt = time.Time{}, time.Time{}
	r.gauge().Dec()
}

// stalledErr ends a stream that waited out maxGapStall, and doubles as the
// operator-facing record of why.
func (r *gapStallReporter) stalledErr(cursor time.Time, detail string) error {
	glog.Errorf("%s subscriber %s %s giving up after %v parked at %v: %s", r.scope, r.clientName, r.pathPrefix,
		r.stalledFor().Truncate(time.Second), cursor, detail)
	return fmt.Errorf("metadata gap at %v did not become readable within %v: %s", cursor, maxGapStall, detail)
}

// close releases the gauge on teardown. Unlike resumed() it does not claim
// recovery: a subscriber that disconnects while parked never resumed.
func (r *gapStallReporter) close() {
	if r.since.IsZero() {
		return
	}
	glog.Warningf("%s subscriber %s %s disconnected after %v parked, still behind", r.scope, r.clientName,
		r.pathPrefix, r.stalledFor().Truncate(time.Second))
	r.gauge().Dec()
	r.since = time.Time{}
}

// gapWaitOutcome says how a park ended.
type gapWaitOutcome int

const (
	gapWaitRetry   gapWaitOutcome = iota // re-probe disk
	gapWaitDone                          // the stream is finished; return nil
	gapWaitStalled                       // stalled past maxGapStall; fail the stream
)

// waitOnGap parks a subscriber that cannot read past a gap. notifyChan may be
// nil, which parks on the retry timer alone - the right choice when no local
// signal corresponds to the event being waited for. The park is where a stalled
// subscriber spends all its time, so every exit the main loop relies on has to
// be checked here too: the loop below it is never reached while parked.
func (fs *FilerServer) waitOnGap(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, cursorTsNs int64, notifyChan <-chan struct{}, stalledFor time.Duration) gapWaitOutcome {
	// A bounded subscription whose window is already behind the gap is finished:
	// LoopProcessLogData, the only place UntilNs ends a stream, is not reachable
	// from here.
	if req.UntilNs != 0 && cursorTsNs > req.UntilNs {
		return gapWaitDone
	}
	if !fs.hasClient(req.ClientId, req.ClientEpoch) {
		return gapWaitDone
	}
	if stalledFor >= maxGapStall {
		return gapWaitStalled
	}
	retry := time.After(unflushedGapRetryInterval)
	for {
		select {
		case _, ok := <-notifyChan:
			if !ok {
				// Closed out from under us: a receive now returns instantly, so
				// stop watching it rather than spinning until the timer fires.
				notifyChan = nil
				continue
			}
		case <-ctx.Done():
			return gapWaitDone
		case <-retry:
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			return gapWaitDone
		}
		return gapWaitRetry
	}
}

// resolveLocalGapResume decides whether a local subscriber may skip a gap its
// disk read found empty. Either proof settles it: nothing after the cursor was
// evicted, so memory still holds the whole gap; or the flush watermark observed
// before the read had already passed the earliest in-memory timestamp, so every
// event in the gap would have been on disk when the read ran and the miss is
// authoritative. Neither needs a wall-clock assumption.
func resolveLocalGapResume(currentTsNs, earliestMemTsNs, flushedTsNs, lastEvictedTsNs int64) (advanceToTsNs int64, advance bool) {
	// No in-memory data (zero time → negative UnixNano), or memory not ahead of us.
	if earliestMemTsNs <= 0 || earliestMemTsNs <= currentTsNs {
		return 0, false
	}
	// The gap may still hold unflushed events.
	if !memoryHoldsGap(currentTsNs, lastEvictedTsNs) && flushedTsNs < earliestMemTsNs {
		return 0, false
	}
	// Resume just below earliest, not at it. A sealed window holding a single
	// entry has startTime == stopTime == earliest, and the sealed-buffer lookup
	// only enters a window whose stopTime is strictly after the cursor, so a
	// cursor sitting exactly on earliest skips that window entirely and loses
	// its sole event. One nanosecond lower takes the startTime.After branch and
	// returns the whole window.
	target := earliestMemTsNs - 1
	if target <= currentTsNs {
		return 0, false
	}
	return target, true
}

func (fs *FilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {
	if fs.filer.MetaAggregator == nil || !fs.filer.MetaAggregator.HasRemotePeers() {
		return fs.SubscribeLocalMetadata(req, stream)
	}

	ctx := stream.Context()
	peerAddress := findClientAddress(ctx, 0)

	isReplacing, alreadyKnown, clientName := fs.addClient("", req.ClientName, peerAddress, req.PathPrefix, req.ClientId, req.ClientEpoch)
	if isReplacing {
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
	} else if alreadyKnown {
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
		return fmt.Errorf("duplicated subscription detected for client %s id %d", clientName, req.ClientId)
	}
	defer func() {
		glog.V(0).Infof("disconnect %v subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("", clientName, req.ClientId, req.ClientEpoch)
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
	}()

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" %v starts to subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

	sender := newPipelinedSender(stream, 1024, req.ClientSupportsBatching)
	defer sender.Close()

	// Register for instant notification when new data arrives in the aggregated log buffer.
	// Used to replace the 1127ms sleep with event-driven wake-up.
	// Key includes clientId/epoch: a replacement stream may reuse the same
	// clientName (same gRPC conn), and sharing the channel would let the old
	// stream's deferred unregister close it under the new stream.
	aggNotifyName := fmt.Sprintf("aggSubscribe:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch)
	// Same key shape for the reader: LoopProcessLogData registers it as a
	// subscriber internally, once per loop iteration.
	aggReaderName := fmt.Sprintf("aggMeta:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch)
	aggNotifyChan := fs.filer.MetaAggregator.MetaLogBuffer.RegisterSubscriber(aggNotifyName)
	defer fs.filer.MetaAggregator.MetaLogBuffer.UnregisterSubscriber(aggNotifyName)

	gapStall := &gapStallReporter{scope: "aggregated", clientName: clientName, pathPrefix: req.PathPrefix}
	defer gapStall.close()

	var unsyncedEvents int64
	eachEventNotificationFn := fs.eachEventNotificationFn(req, sender, clientName, &unsyncedEvents)

	// lastSeenTsNs tracks how far the subscriber has read so idle heartbeats are
	// only emitted once it is caught up to the buffer head. It is read and
	// written from this single goroutine, so no synchronization is needed.
	var lastSeenTsNs int64
	var lastHeartbeatNs int64
	baseEachLogEntryFn := eachLogEntryFn(req, sender, eachEventNotificationFn, &unsyncedEvents)
	eachLogEntryFn := func(logEntry *filer_pb.LogEntry) (bool, error) {
		lastSeenTsNs = logEntry.TsNs
		return baseEachLogEntryFn(logEntry)
	}

	var processedTsNs int64
	var readPersistedLogErr error
	var readInMemoryLogErr error
	var isDone bool

	for {

		glog.V(4).Infof("read on disk %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		cursorBeforeDiskTsNs := lastReadTime.Time.UnixNano()
		evictedBeforeDiskTsNs := fs.filer.MetaAggregator.MetaLogBuffer.GetLastEvictedTsNs()

		if req.ClientSupportsMetadataChunks {
			processedTsNs, isDone, readPersistedLogErr = fs.sendLogFileRefs(ctx, sender, lastReadTime, req.UntilNs)
		} else {
			processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(ctx, lastReadTime, req.UntilNs, eachLogEntryFn)
		}
		if readPersistedLogErr != nil {
			return fmt.Errorf("reading from persisted logs: %w", readPersistedLogErr)
		}
		if isDone {
			return nil
		}

		glog.V(4).Infof("processed to %v: %v", clientName, processedTsNs)
		if diskReadAdvanced(processedTsNs, lastReadTime) {
			gapStall.resumed()
			reportUnprovenAggregatedCrossing(cursorBeforeDiskTsNs, processedTsNs, evictedBeforeDiskTsNs, clientName, req.PathPrefix)
			lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
		} else if !errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
			// First pass or no ResumeFromDiskError yet - check the next day for logs
			nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
			position := log_buffer.NewMessagePosition(nextDayTs, -2)
			found, err := fs.filer.HasPersistedLogFiles(position)
			if err != nil {
				return fmt.Errorf("checking persisted log files: %w", err)
			}
			if found {
				lastReadTime = position
			}
		}

		// The ring drops sealed windows this buffer never persists (peers write
		// their own logs), so reading memory at a cursor the ring has already
		// passed would jump silently over whatever it dropped. Resolve that here
		// rather than inside ReadFromBuffer, which is shared with the message
		// queue and has no gap handling of its own. Skip only what the eviction
		// watermark proves empty; otherwise wait for a peer's flush to put the
		// window on disk.
		earliestTime := fs.filer.MetaAggregator.MetaLogBuffer.GetEarliestTime()
		lastEvictedTsNs := fs.filer.MetaAggregator.MetaLogBuffer.GetLastEvictedTsNs()
		memoryIncomplete := !memoryHoldsGap(lastReadTime.Time.UnixNano(), lastEvictedTsNs)
		diskExhausted := !diskReadAdvanced(processedTsNs, lastReadTime) && errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError)
		if memoryIncomplete || diskExhausted {
			if advanceToTsNs, advance := resolveAggregatedGapResume(lastReadTime.Time.UnixNano(), earliestTime.UnixNano(), lastEvictedTsNs); advance {
				gapStall.resumed()
				glog.V(3).Infof("gap detected: skipping from %v to earliest memory time %v for %v",
					lastReadTime.Time, earliestTime, clientName)
				lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
				readInMemoryLogErr = nil // Reached the in-memory window: resume from memory
			} else {
				// An append tells a subscriber waiting on a peer's flush nothing,
				// and each wake re-runs a full ReadPersistedLogBuffer, so that
				// wait is paced by the timer alone. An empty ring is the one case
				// where new data is exactly the signal, so it keeps the channel.
				var notifyChan <-chan struct{}
				reason := fmt.Sprintf("gap evicted through %v is not on a peer's disk yet (earliest memory %v)",
					time.Unix(0, lastEvictedTsNs), earliestTime)
				if !memoryIncomplete {
					notifyChan = aggNotifyChan
					reason = "aggregated buffer has no readable entries yet"
				}
				gapStall.park(lastReadTime.Time, reason)
				switch fs.waitOnGap(ctx, req, lastReadTime.Time.UnixNano(), notifyChan, gapStall.stalledFor()) {
				case gapWaitDone:
					return nil
				case gapWaitStalled:
					return gapStall.stalledErr(lastReadTime.Time, reason)
				}
				continue
			}
		}

		glog.V(4).Infof("read in memory %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		lastReadTime, isDone, readInMemoryLogErr = fs.filer.MetaAggregator.MetaLogBuffer.LoopProcessLogData(aggReaderName, lastReadTime, req.UntilNs, func() bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			if !fs.hasClient(req.ClientId, req.ClientEpoch) {
				return false
			}
			lastHeartbeatNs = fs.maybeSendIdleHeartbeat(req, sender, fs.filer.MetaAggregator.MetaLogBuffer, lastReadTime.Time.UnixNano(), lastSeenTsNs, lastHeartbeatNs)
			return true
		}, eachLogEntryFn)
		if readInMemoryLogErr != nil {
			if errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
				// Memory says data is too old - will read from disk on next iteration
				// But if disk also has no data (gap in history), we'll skip forward
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if !errors.Is(readInMemoryLogErr, log_buffer.ResumeError) {
				break
			}
		}
		if isDone {
			return nil
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			glog.V(0).Infof("client %v is closed", clientName)
			return nil
		}

		// Wait for new data (event-driven instead of 1127ms polling).
		// Drain any stale notification first to avoid a spurious wake-up.
		select {
		case <-aggNotifyChan:
		default:
		}
		select {
		case <-aggNotifyChan:
		case <-ctx.Done():
			return nil
		}
	}

	return readInMemoryLogErr

}

func (fs *FilerServer) SubscribeLocalMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeLocalMetadataServer) error {

	ctx := stream.Context()
	peerAddress := findClientAddress(ctx, 0)

	// use negative client id to differentiate from addClient()/deleteClient() used in SubscribeMetadata()
	req.ClientId = -req.ClientId

	isReplacing, alreadyKnown, clientName := fs.addClient("local", req.ClientName, peerAddress, req.PathPrefix, req.ClientId, req.ClientEpoch)
	if isReplacing {
	} else if alreadyKnown {
		return fmt.Errorf("duplicated local subscription detected for client %s clientId:%d", clientName, req.ClientId)
	}
	defer func() {
		glog.V(0).Infof("disconnect %v local subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("local", clientName, req.ClientId, req.ClientEpoch)
	}()

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" + %v local subscribe %s from %+v clientId:%d", clientName, req.PathPrefix, lastReadTime, req.ClientId)

	sender := newPipelinedSender(stream, 1024, req.ClientSupportsBatching)
	defer sender.Close()

	// Bounded gap waits use the buffer's subscriber notification plus a retry
	// timer, so a flush landing between the disk read and the wait cannot
	// strand the subscriber (no lost-wakeup window). Key includes clientId/
	// epoch so a replacement stream never shares (and loses) the channel.
	localNotifyName := fmt.Sprintf("localGap:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch)
	// Same key shape for the reader: LoopProcessLogData registers it as a
	// subscriber internally, once per loop iteration.
	localReaderName := fmt.Sprintf("localMeta:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch)
	localFlushChan := fs.filer.LocalMetaLogBuffer.RegisterFlushSubscriber(localNotifyName)
	defer fs.filer.LocalMetaLogBuffer.UnregisterFlushSubscriber(localNotifyName)

	gapStall := &gapStallReporter{scope: "local", clientName: clientName, pathPrefix: req.PathPrefix}
	defer gapStall.close()

	var unsyncedEvents int64
	eachEventNotificationFn := fs.eachEventNotificationFn(req, sender, clientName, &unsyncedEvents)

	// lastSeenTsNs tracks how far the subscriber has read so idle heartbeats are
	// only emitted once it is caught up to the buffer head. It is read and
	// written from this single goroutine, so no synchronization is needed.
	var lastSeenTsNs int64
	var lastHeartbeatNs int64
	baseEachLogEntryFn := eachLogEntryFn(req, sender, eachEventNotificationFn, &unsyncedEvents)
	eachLogEntryFn := func(logEntry *filer_pb.LogEntry) (bool, error) {
		lastSeenTsNs = logEntry.TsNs
		return baseEachLogEntryFn(logEntry)
	}

	var processedTsNs int64
	var readPersistedLogErr error
	var readInMemoryLogErr error
	var isDone bool
	var lastCheckedFlushTsNs int64 = -1 // Track the last flushed time we checked
	var lastDiskReadTsNs int64 = -1     // Track the last read position we used for disk read

	for {
		// Check if new data has been flushed to disk since last check, or if read position advanced
		currentFlushTsNs := fs.filer.LocalMetaLogBuffer.GetLastFlushTsNs()
		currentReadTsNs := lastReadTime.Time.UnixNano()
		// Read from disk if: first time, new flush observed, or read position advanced (draining backlog)
		shouldReadFromDisk := lastCheckedFlushTsNs == -1 ||
			currentFlushTsNs > lastCheckedFlushTsNs ||
			currentReadTsNs > lastDiskReadTsNs

		if shouldReadFromDisk {
			// Record the position we are about to read from
			lastDiskReadTsNs = currentReadTsNs
			glog.V(4).Infof("read on disk %v local subscribe %s from %+v (lastFlushed: %v)", clientName, req.PathPrefix, lastReadTime, time.Unix(0, currentFlushTsNs))
			if req.ClientSupportsMetadataChunks {
				processedTsNs, isDone, readPersistedLogErr = fs.sendLogFileRefs(ctx, sender, lastReadTime, req.UntilNs)
			} else {
				processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(ctx, lastReadTime, req.UntilNs, eachLogEntryFn)
			}
			if readPersistedLogErr != nil {
				glog.V(0).Infof("read on disk %v local subscribe %s from %+v: %v", clientName, req.PathPrefix, lastReadTime, readPersistedLogErr)
				return fmt.Errorf("reading from persisted logs: %w", readPersistedLogErr)
			}
			if isDone {
				return nil
			}

			// Update the last checked flushed time
			lastCheckedFlushTsNs = currentFlushTsNs

			if diskReadAdvanced(processedTsNs, lastReadTime) {
				gapStall.resumed()
				lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
			} else {
				// No data found on disk
				// Check if we previously got ResumeFromDiskError from memory, meaning we're in a gap
				if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
					// The read above ran after observing the currentFlushTsNs
					// watermark and found nothing: once that watermark has passed
					// the earliest in-memory time, the gap is provably empty.
					earliestTime := fs.filer.LocalMetaLogBuffer.GetEarliestTime()
					lastEvictedTsNs := fs.filer.LocalMetaLogBuffer.GetLastEvictedTsNs()
					if advanceToTsNs, advance := resolveLocalGapResume(lastReadTime.Time.UnixNano(), earliestTime.UnixNano(), currentFlushTsNs, lastEvictedTsNs); advance {
						gapStall.resumed()
						glog.V(3).Infof("gap detected: skipping from %v to flushed earliest memory time %v for %v",
							lastReadTime.Time, earliestTime, clientName)
						lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
						readInMemoryLogErr = nil // Clear the error since we're skipping forward
					} else {
						// The gap may hold unflushed events: wait (bounded) for
						// the flush, then re-read disk.
						reason := fmt.Sprintf("gap is not flushed yet (earliest memory %v, flushed through %v)",
							earliestTime, time.Unix(0, currentFlushTsNs))
						gapStall.park(lastReadTime.Time, reason)
						switch fs.waitOnGap(ctx, req, lastReadTime.Time.UnixNano(), localFlushChan, gapStall.stalledFor()) {
						case gapWaitDone:
							return nil
						case gapWaitStalled:
							return gapStall.stalledErr(lastReadTime.Time, reason)
						}
						continue
					}
				} else {
					// First pass or no ResumeFromDiskError yet
					// Check the next day for logs
					nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
					position := log_buffer.NewMessagePosition(nextDayTs, -2)
					found, err := fs.filer.HasPersistedLogFiles(position)
					if err != nil {
						return fmt.Errorf("checking persisted log files: %w", err)
					}
					if found {
						lastReadTime = position
					}
				}
			}
		}

		// Same reason as the aggregated path: a cursor the ring has already
		// passed cannot be served from memory without skipping what it dropped,
		// and ReadFromBuffer is shared with the message queue and has no gap
		// handling of its own. The disk read above may also have left the cursor
		// short of the watermark, so this is checked every pass, not only when
		// the disk came up empty.
		if lastEvictedTsNs := fs.filer.LocalMetaLogBuffer.GetLastEvictedTsNs(); !memoryHoldsGap(lastReadTime.Time.UnixNano(), lastEvictedTsNs) {
			earliestTime := fs.filer.LocalMetaLogBuffer.GetEarliestTime()
			if advanceToTsNs, advance := resolveLocalGapResume(lastReadTime.Time.UnixNano(), earliestTime.UnixNano(), lastCheckedFlushTsNs, lastEvictedTsNs); advance {
				gapStall.resumed()
				lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
				readInMemoryLogErr = nil
			} else {
				reason := fmt.Sprintf("gap evicted through %v is not flushed yet (earliest memory %v, flushed through %v)",
					time.Unix(0, lastEvictedTsNs), earliestTime, time.Unix(0, lastCheckedFlushTsNs))
				gapStall.park(lastReadTime.Time, reason)
				switch fs.waitOnGap(ctx, req, lastReadTime.Time.UnixNano(), localFlushChan, gapStall.stalledFor()) {
				case gapWaitDone:
					return nil
				case gapWaitStalled:
					return gapStall.stalledErr(lastReadTime.Time, reason)
				}
				continue
			}
		}

		glog.V(3).Infof("read in memory %v local subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		lastReadTime, isDone, readInMemoryLogErr = fs.filer.LocalMetaLogBuffer.LoopProcessLogData(localReaderName, lastReadTime, req.UntilNs, func() bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			if !fs.hasClient(req.ClientId, req.ClientEpoch) {
				return false
			}
			lastHeartbeatNs = fs.maybeSendIdleHeartbeat(req, sender, fs.filer.LocalMetaLogBuffer, lastReadTime.Time.UnixNano(), lastSeenTsNs, lastHeartbeatNs)
			return true
		}, eachLogEntryFn)
		if readInMemoryLogErr != nil {
			if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
				// Memory buffer says the requested time is too old
				// Retry disk read if: (a) flush advanced, or (b) read position advanced (draining backlog)
				currentFlushTsNs := fs.filer.LocalMetaLogBuffer.GetLastFlushTsNs()
				currentReadTsNs := lastReadTime.Time.UnixNano()
				if currentFlushTsNs > lastCheckedFlushTsNs || currentReadTsNs > lastDiskReadTsNs {
					glog.V(0).Infof("retry disk read %v local subscribe %s (lastFlushed: %v -> %v, readTs: %v -> %v)",
						clientName, req.PathPrefix,
						time.Unix(0, lastCheckedFlushTsNs), time.Unix(0, currentFlushTsNs),
						time.Unix(0, lastDiskReadTsNs), time.Unix(0, currentReadTsNs))
					continue
				}
				// No flush or read-position progress since the last disk read: that
				// read already proved everything up to the lastCheckedFlushTsNs
				// watermark, so skip only if it covers the earliest in-memory time.
				earliestTime := fs.filer.LocalMetaLogBuffer.GetEarliestTime()
				lastEvictedTsNs := fs.filer.LocalMetaLogBuffer.GetLastEvictedTsNs()
				if advanceToTsNs, advance := resolveLocalGapResume(currentReadTsNs, earliestTime.UnixNano(), lastCheckedFlushTsNs, lastEvictedTsNs); advance {
					gapStall.resumed()
					glog.V(3).Infof("gap detected: skipping from %v to flushed earliest memory time %v for %v",
						lastReadTime.Time, earliestTime, clientName)
					lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
					// Clear the error so the next iteration re-reads disk.
					readInMemoryLogErr = nil
					continue
				}
				// The gap may hold unflushed events: wait (bounded) for the
				// flush, then re-evaluate.
				reason := fmt.Sprintf("gap is not flushed yet (earliest memory %v, flushed through %v)",
					earliestTime, time.Unix(0, lastCheckedFlushTsNs))
				gapStall.park(lastReadTime.Time, reason)
				switch fs.waitOnGap(ctx, req, lastReadTime.Time.UnixNano(), localFlushChan, gapStall.stalledFor()) {
				case gapWaitDone:
					return nil
				case gapWaitStalled:
					return gapStall.stalledErr(lastReadTime.Time, reason)
				}
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if readInMemoryLogErr != log_buffer.ResumeError {
				break
			}
		}
		if isDone {
			return nil
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			return nil
		}
	}

	return readInMemoryLogErr

}

func eachLogEntryFn(req *filer_pb.SubscribeMetadataRequest, sender metadataStreamSender, eachEventNotificationFn func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error, filtered *int64) log_buffer.EachLogEntryFuncType {
	// A shallow scan of the path fields skips unmarshaling chunk-heavy events
	// this subscriber would filter out anyway; scan surprises fall back to the
	// full decode. Only a delivery resets the shared unsynced-events counter.
	prefilter := req.PathPrefix != "" || len(req.PathPrefixes) > 0 || len(req.Directories) > 0
	return func(logEntry *filer_pb.LogEntry) (bool, error) {
		if prefilter {
			if skeleton, ok := filer_pb.ScanMetadataEventSkeleton(logEntry.Data); ok &&
				!filer_pb.MetadataEventMatchesSubscription(skeleton, req.PathPrefix, req.PathPrefixes, req.Directories) {
				*filtered++
				if *filtered > MaxUnsyncedEvents {
					if err := sender.Send(&filer_pb.SubscribeMetadataResponse{
						EventNotification: &filer_pb.EventNotification{},
						TsNs:              skeleton.TsNs,
					}); err != nil {
						return false, err
					}
					*filtered = 0
				}
				return false, nil
			}
		}
		event := &filer_pb.SubscribeMetadataResponse{}
		// proto.Unmarshal (not UnmarshalVT) validates UTF-8 in string fields, so
		// malformed metadata is rejected here instead of reaching path filtering
		// and subscribers.
		if err := proto.Unmarshal(logEntry.Data, event); err != nil {
			glog.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
			return false, fmt.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %w", err)
		}

		if err := eachEventNotificationFn(event.Directory, event.EventNotification, event.TsNs); err != nil {
			return false, err
		}

		return false, nil
	}
}

// maybeSendIdleHeartbeat emits an empty response carrying the current time when
// the subscriber has consumed everything up to the buffer head. The client uses
// it to advance freshness signals (e.g. filer.sync's sync_offset) without moving
// its resume checkpoint, so a restart still re-reads from the last real event.
//
// The catch-up floor is the max of two read-progress markers:
//   - readPositionTsNs: how far the read cursor has advanced. It starts at
//     SinceNs and also covers metadata-chunks mode, where persisted entries are
//     replayed as log file refs rather than through eachLogEntryFn.
//   - lastSeenTsNs: the timestamp of the most recent entry streamed in this
//     call. It advances live while reading the in-memory backlog, before the
//     read cursor returned by LoopProcessLogData has been updated.
//
// While the buffer head is past that floor the subscriber is still behind (e.g.
// replaying a backlog) and no heartbeat is sent. Returns the (possibly advanced)
// lastHeartbeatNs.
func (fs *FilerServer) maybeSendIdleHeartbeat(req *filer_pb.SubscribeMetadataRequest, sender metadataStreamSender, logBuffer *log_buffer.LogBuffer, readPositionTsNs, lastSeenTsNs, lastHeartbeatNs int64) int64 {
	if !req.ClientSupportsIdleHeartbeat {
		return lastHeartbeatNs
	}
	floorTsNs := lastSeenTsNs
	if readPositionTsNs > floorTsNs {
		floorTsNs = readPositionTsNs
	}
	if logBuffer.LastTsNs.Load() > floorTsNs {
		// the buffer holds data the subscriber has not reached yet
		return lastHeartbeatNs
	}
	now := time.Now().UnixNano()
	if now-lastHeartbeatNs < int64(idleHeartbeatInterval) {
		return lastHeartbeatNs
	}
	if err := sender.Send(&filer_pb.SubscribeMetadataResponse{TsNs: now}); err != nil {
		glog.V(0).Infof("=> idle heartbeat to %s: %v", req.ClientName, err)
		return lastHeartbeatNs
	}
	// A heartbeat is a send too: advance the freshness gauge so an idle but
	// healthy subscriber doesn't look stale. The gauge otherwise only moves on
	// real matching events, which never arrive on a quiet path.
	var sourceFiler string
	if fs.option != nil {
		sourceFiler = fs.option.Host.String()
	}
	stats.FilerServerLastSendTsOfSubscribeGauge.WithLabelValues(sourceFiler, req.ClientName, req.PathPrefix).Set(float64(now))
	return now
}

// sendLogFileRefs collects persisted log file chunk references and sends them
// to the client so it can read the data directly from volume servers.
// This does zero volume server I/O — it only lists filer store directory entries.
// Refs go through the pipelinedSender like every other message: gRPC allows
// only one goroutine to Send on a stream, and the sender's goroutine is it.
// The sender keeps refs messages out of Events batches.
func (fs *FilerServer) sendLogFileRefs(ctx context.Context, stream metadataStreamSender, startPosition log_buffer.MessagePosition, stopTsNs int64) (lastTsNs int64, isDone bool, err error) {
	refs, lastTsNs, err := fs.filer.CollectLogFileRefs(ctx, startPosition, stopTsNs)
	if err != nil {
		return 0, false, err
	}
	if len(refs) == 0 {
		return 0, false, nil
	}

	const maxRefsPerMessage = 64
	for i := 0; i < len(refs); i += maxRefsPerMessage {
		end := i + maxRefsPerMessage
		if end > len(refs) {
			end = len(refs)
		}
		if err := stream.Send(&filer_pb.SubscribeMetadataResponse{
			LogFileRefs: refs[i:end],
		}); err != nil {
			return lastTsNs, false, err
		}
	}
	return lastTsNs, false, nil
}

func (fs *FilerServer) eachEventNotificationFn(req *filer_pb.SubscribeMetadataRequest, sender metadataStreamSender, clientName string, filtered *int64) func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
	return func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		defer func() {
			if *filtered > MaxUnsyncedEvents {
				if err := sender.Send(&filer_pb.SubscribeMetadataResponse{
					EventNotification: &filer_pb.EventNotification{},
					TsNs:              tsNs,
				}); err == nil {
					*filtered = 0
				}
			}
		}()

		*filtered++
		foundSelf := false
		for _, sig := range eventNotification.Signatures {
			if sig == req.Signature && req.Signature != 0 {
				return nil
			}
			if sig == fs.filer.Signature {
				foundSelf = true
			}
		}
		if !foundSelf {
			eventNotification.Signatures = append(eventNotification.Signatures, fs.filer.Signature)
		}

		// get complete path to the file or directory
		var entryName string
		if eventNotification.OldEntry != nil {
			entryName = eventNotification.OldEntry.Name
		} else if eventNotification.NewEntry != nil {
			entryName = eventNotification.NewEntry.Name
		}

		fullpath := util.Join(dirPath, entryName)

		// skip on filer internal meta logs
		if strings.HasPrefix(fullpath, filer.SystemLogDir) {
			return nil
		}

		message := &filer_pb.SubscribeMetadataResponse{
			Directory:         dirPath,
			EventNotification: eventNotification,
			TsNs:              tsNs,
		}

		if !filer_pb.MetadataEventMatchesSubscription(message, req.PathPrefix, req.PathPrefixes, req.Directories) {
			return nil
		}

		// collect timestamps for path
		stats.FilerServerLastSendTsOfSubscribeGauge.WithLabelValues(fs.option.Host.String(), req.ClientName, req.PathPrefix).Set(float64(tsNs))

		// println("sending", dirPath, entryName)
		if err := sender.Send(message); err != nil {
			glog.V(0).Infof("=> client %v: %+v", clientName, err)
			return err
		}
		*filtered = 0
		return nil
	}
}

func (fs *FilerServer) addClient(scope string, clientType string, clientAddress string, pathPrefix string, clientId int32, clientEpoch int32) (isReplacing, alreadyKnown bool, clientName string) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ %v listener %v clientId %v clientEpoch %v", scope, clientName, clientId, clientEpoch)
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if !found || epoch < clientEpoch {
			fs.knownListeners[clientId] = clientEpoch
			isReplacing = true
			if fs.subscribers == nil {
				fs.subscribers = make(map[int32]*metadataSubscriber)
			}
			fs.subscribers[clientId] = &metadataSubscriber{
				clientName:    clientName,
				clientType:    clientType,
				address:       clientAddress,
				pathPrefix:    pathPrefix,
				clientId:      clientId,
				clientEpoch:   clientEpoch,
				connectedAtNs: time.Now().UnixNano(),
			}
		} else {
			alreadyKnown = true
		}
	}
	return
}

func (fs *FilerServer) deleteClient(scope string, clientName string, clientId int32, clientEpoch int32) {
	glog.V(0).Infof("- %v listener %v clientId %v clientEpoch %v", scope, clientName, clientId, clientEpoch)
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			delete(fs.knownListeners, clientId)
			delete(fs.subscribers, clientId)
		}
	}
}

func (fs *FilerServer) hasClient(clientId int32, clientEpoch int32) bool {
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			return true
		}
	}
	return false
}
