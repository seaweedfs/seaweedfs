package weed_server

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	// readable before giving up and skipping it. Waiting is productive while
	// the window holding the gap is still queued for a flush, but a peer that
	// never comes back - or one whose filer store this filer cannot read at
	// all - makes the wait permanent, and a subscriber that stops delivering
	// for good is worse than one that records a bounded loss and moves on.
	// Failing the stream instead just moves the loop into the client, which
	// reconnects at the same position and hits the same wall. The skip is
	// counted and logged at error level; master skipped the same ranges
	// immediately and silently.
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
	stats.FilerSubscribeUnprovenGapCrossings.WithLabelValues("aggregated").Inc()
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

// gaveUp records that the subscriber stopped waiting on an unprovable gap and
// skipped it. This is the loss the whole gap machinery exists to make loud: it
// shares the unproven-crossing counter and logs at error level.
func (r *gapStallReporter) gaveUp(cursor time.Time, skipToTsNs int64, detail string) {
	stats.FilerSubscribeUnprovenGapCrossings.WithLabelValues(r.scope).Inc()
	glog.Errorf("%s subscriber %s %s skipping the gap (%v..%v] after %v parked: %s; events a peer flushes into that range later will not be delivered",
		r.scope, r.clientName, r.pathPrefix, cursor, time.Unix(0, skipToTsNs), r.stalledFor().Truncate(time.Second), detail)
	r.since, r.lastWarnAt = time.Time{}, time.Time{}
	r.gauge().Dec()
}

// restartStall re-arms the stall clock for a park that outlived maxGapStall
// with nothing to skip to, so the give-up path does not retrigger on every
// retry while still reporting each full cycle.
func (r *gapStallReporter) restartStall(cursor time.Time, detail string) {
	glog.Errorf("%s subscriber %s %s still parked after %v at %v with nothing to skip to: %s", r.scope, r.clientName,
		r.pathPrefix, r.stalledFor().Truncate(time.Second), cursor, detail)
	r.since, r.lastWarnAt = time.Now(), time.Time{}
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

// parkOnGap parks the subscriber on a gap it cannot read past and reports how
// to go on. done: the stream is over - the client is gone, a bounded
// subscription is complete, or the context ended. skip: the park outlived
// maxGapStall and the caller must resume at skipToTsNs, abandoning the gap
// (recorded via gaveUp). Otherwise the caller re-probes. notifyChan may be nil,
// which parks on the retry timer alone - right when no local signal
// corresponds to the event being waited for. The park is where a stalled
// subscriber spends all its time, so every exit the read loop relies on has to
// be checked here too.
func (fs *FilerServer) parkOnGap(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, gapStall *gapStallReporter, evictedTsNs func() int64, cursor log_buffer.MessagePosition, notifyChan <-chan struct{}, reason string) (skipToTsNs int64, skip bool, done bool) {
	// The done exits run before park(): a stream that is already finished was
	// never parked, and marking it so leaves a phantom gauge blip and a false
	// "disconnected still behind" trace on a healthy completion.
	//
	// A bounded subscription whose cursor reached UntilNs is finished:
	// LoopProcessLogData, the only place UntilNs ends a stream, is unreachable
	// from here, and the bound is inclusive while cursors are exclusive.
	if req.UntilNs != 0 && cursor.Time.UnixNano() >= req.UntilNs {
		return 0, false, true
	}
	if !fs.hasClient(req.ClientId, req.ClientEpoch) {
		return 0, false, true
	}
	gapStall.park(cursor.Time, reason)
	if gapStall.stalledFor() >= maxGapStall {
		// Resume at the eviction watermark: everything retained starts
		// strictly after it, so the recorded loss is exactly (cursor, skipTo].
		if evicted := evictedTsNs(); evicted > cursor.Time.UnixNano() {
			gapStall.gaveUp(cursor.Time, evicted, reason)
			return evicted, true, false
		}
		// Nothing was withheld past the cursor - nothing to skip, nothing being
		// lost; keep waiting on a fresh stall cycle.
		gapStall.restartStall(cursor.Time, reason)
	}
	// Re-probes back off as the stall ages: every retry re-reads the persisted
	// log, and probing the store each 2s for 15 minutes - per parked subscriber,
	// during the outage that parked them - makes the bad time worse.
	waitFor := unflushedGapRetryInterval + gapStall.stalledFor()/8
	if waitFor > gapStallWarnInterval {
		waitFor = gapStallWarnInterval
	}
	retry := time.After(waitFor)
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
			return 0, false, true
		case <-retry:
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			return 0, false, true
		}
		return 0, false, false
	}
}

// resolveGapResume decides whether a subscriber may skip a gap its disk read
// found empty. Either proof settles it: nothing after the cursor was evicted,
// so memory still holds the whole gap; or the flush watermark observed before
// the read had already passed the earliest in-memory timestamp, so every event
// in the gap would have been on disk when the read ran and the miss is
// authoritative. The aggregated ring never flushes - peers persist their own
// logs - so it passes flushedTsNs 0 and only the eviction proof can hold.
func resolveGapResume(currentTsNs, currentOffset, earliestMemTsNs, flushedTsNs, lastEvictedTsNs int64) (advanceToTsNs int64, advance bool) {
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
	if target < currentTsNs {
		return 0, false
	}
	if target == currentTsNs && currentOffset <= 0 {
		// The sentinel resume would be the position we already hold.
		return 0, false
	}
	// target > cursor is plainly forward. target == cursor with a positive
	// (exclusive) offset is progress too: that cursor cannot be served -
	// ReadFromBuffer refuses positive offsets below the window - while the
	// sentinel one is, and both deliver exactly the entries after target.
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
	} else if alreadyKnown {
		return fmt.Errorf("duplicated subscription detected for client %s id %d", clientName, req.ClientId)
	}
	defer func() {
		glog.V(0).Infof("disconnect %v subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("", clientName, req.ClientId, req.ClientEpoch)
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
	refsSentAtTsNs := int64(math.MinInt64) // position refs were last sent for; never re-sent at the same one

	for {

		glog.V(4).Infof("read on disk %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		cursorBeforeDiskTsNs := lastReadTime.Time.UnixNano()

		// One disk pass. Chunk-capable clients get refs, but only once per
		// position: gap retries run this loop every couple of seconds, and
		// re-sending the same refs piles duplicates into the client's pending
		// list, which it only drains when a non-ref message arrives. Refs also
		// name files by their start minute, below the content the client was
		// actually given, so when they cannot advance the cursor an entry pass
		// moves it by real entry timestamps instead.
		if req.ClientSupportsMetadataChunks && cursorBeforeDiskTsNs != refsSentAtTsNs {
			refsSentAtTsNs = cursorBeforeDiskTsNs
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
		diskAdvanced := diskReadAdvanced(processedTsNs, lastReadTime)
		// The watermark is read after the disk read: an eviction landing while
		// a long backlog read runs must count against the position it produced.
		lastEvictedTsNs := fs.filer.MetaAggregator.MetaLogBuffer.GetLastEvictedTsNs()
		if diskAdvanced {
			gapStall.resumed()
			reportUnprovenAggregatedCrossing(cursorBeforeDiskTsNs, processedTsNs, lastEvictedTsNs, clientName, req.PathPrefix)
			lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
		} else if readInMemoryLogErr == nil {
			// Nothing on disk and memory never spoke: scan forward for the next
			// day that has logs.
			nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
			position := log_buffer.NewMessagePosition(nextDayTs, -2)
			found, err := fs.filer.HasPersistedLogFiles(position)
			if err != nil {
				return fmt.Errorf("checking persisted log files: %w", err)
			}
			if found {
				gapStall.resumed()
				reportUnprovenAggregatedCrossing(cursorBeforeDiskTsNs, nextDayTs, lastEvictedTsNs, clientName, req.PathPrefix)
				lastReadTime = position
			}
		}

		// Reading memory at a cursor the ring has evicted past would silently
		// jump whatever the ring dropped, so it is resolved here on every pass
		// - ReadFromBuffer is shared with the message queue and has no gap
		// handling of its own.
		earliestTime := fs.filer.MetaAggregator.MetaLogBuffer.GetEarliestTime()
		if !memoryHoldsGap(lastReadTime.Time.UnixNano(), lastEvictedTsNs) {
			if diskAdvanced {
				// The disk may hold more of the gap: keep reading it.
				continue
			}
			if advanceToTsNs, advance := resolveGapResume(lastReadTime.Time.UnixNano(), lastReadTime.Offset, earliestTime.UnixNano(), 0, lastEvictedTsNs); advance {
				gapStall.resumed()
				glog.V(3).Infof("gap detected: skipping from %v to earliest memory time %v for %v",
					lastReadTime.Time, earliestTime, clientName)
				lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
				readInMemoryLogErr = nil
			} else {
				// An append tells a subscriber waiting on a peer's flush
				// nothing, and each retry re-reads the persisted logs anyway,
				// so this wait is paced by the timer alone.
				reason := fmt.Sprintf("gap evicted through %v is not on a peer's disk yet (earliest memory %v)",
					time.Unix(0, lastEvictedTsNs), earliestTime)
				if skipTo, skip, done := fs.parkOnGap(ctx, req, gapStall, fs.filer.MetaAggregator.MetaLogBuffer.GetLastEvictedTsNs, lastReadTime, nil, reason); done {
					return nil
				} else if skip {
					lastReadTime = log_buffer.NewMessagePosition(skipTo, gapResumeCursorOffset)
					readInMemoryLogErr = nil
				}
				continue
			}
		} else if !diskAdvanced && errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
			// The last memory read refused this cursor, the disk just proved it
			// has nothing more, yet nothing after the cursor was evicted: the
			// cursor's exclusive offset predates the retained window. Re-arm it
			// onto the window; failing even that, wait for data to arrive.
			if advanceToTsNs, advance := resolveGapResume(lastReadTime.Time.UnixNano(), lastReadTime.Offset, earliestTime.UnixNano(), 0, lastEvictedTsNs); advance {
				gapStall.resumed()
				lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
				readInMemoryLogErr = nil
			} else {
				if skipTo, skip, done := fs.parkOnGap(ctx, req, gapStall, fs.filer.MetaAggregator.MetaLogBuffer.GetLastEvictedTsNs, lastReadTime, aggNotifyChan, "aggregated buffer has no readable entries yet"); done {
					return nil
				} else if skip {
					lastReadTime = log_buffer.NewMessagePosition(skipTo, gapResumeCursorOffset)
					readInMemoryLogErr = nil
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
				// Fell behind the ring: back to the disk pass, and from there to
				// the gap resolution above if the disk has nothing either.
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
	var lastCheckedFlushTsNs int64 = -1    // Track the last flushed time we checked
	var lastDiskReadTsNs int64 = -1        // Track the last read position we used for disk read
	refsSentAtTsNs := int64(math.MinInt64) // position refs were last sent for; never re-sent at the same one

	for {
		// Check if new data has been flushed to disk since last check, or if read position advanced
		currentFlushTsNs := fs.filer.LocalMetaLogBuffer.GetLastFlushTsNs()
		currentReadTsNs := lastReadTime.Time.UnixNano()
		// Read from disk if: first time, new flush observed, or read position advanced (draining backlog)
		shouldReadFromDisk := lastCheckedFlushTsNs == -1 ||
			currentFlushTsNs > lastCheckedFlushTsNs ||
			currentReadTsNs > lastDiskReadTsNs

		diskAdvanced := false
		if shouldReadFromDisk {
			// Record the position we are about to read from
			lastDiskReadTsNs = currentReadTsNs
			glog.V(4).Infof("read on disk %v local subscribe %s from %+v (lastFlushed: %v)", clientName, req.PathPrefix, lastReadTime, time.Unix(0, currentFlushTsNs))
			// Chunk-capable clients get refs, but only once per position: gap
			// retries re-run this pass, and re-sent refs pile up in the
			// client's pending list, which it only drains on a non-ref
			// message. Refs also name files by their start minute, below the
			// content the client was actually given, so when they cannot
			// advance the cursor an entry pass moves it by real timestamps.
			if req.ClientSupportsMetadataChunks && currentReadTsNs != refsSentAtTsNs {
				refsSentAtTsNs = currentReadTsNs
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

			diskAdvanced = diskReadAdvanced(processedTsNs, lastReadTime)
			if diskAdvanced {
				gapStall.resumed()
				lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
			} else if readInMemoryLogErr == nil {
				// Nothing on disk and memory never spoke: scan forward for the
				// next day that has logs.
				nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
				position := log_buffer.NewMessagePosition(nextDayTs, -2)
				found, err := fs.filer.HasPersistedLogFiles(position)
				if err != nil {
					return fmt.Errorf("checking persisted log files: %w", err)
				}
				if found {
					gapStall.resumed()
					lastReadTime = position
				}
			}
		}

		// Reading memory at a cursor the ring has evicted past would silently
		// jump whatever the ring dropped, so it is resolved here on every pass
		// - ReadFromBuffer is shared with the message queue and has no gap
		// handling of its own. Unlike the aggregated ring this buffer flushes,
		// so a disk miss observed at a flush watermark past the earliest
		// in-memory time is a second, independent proof the gap is empty.
		earliestTime := fs.filer.LocalMetaLogBuffer.GetEarliestTime()
		lastEvictedTsNs := fs.filer.LocalMetaLogBuffer.GetLastEvictedTsNs()
		if !memoryHoldsGap(lastReadTime.Time.UnixNano(), lastEvictedTsNs) {
			if diskAdvanced {
				// The disk may hold more of the gap: keep reading it.
				continue
			}
			if advanceToTsNs, advance := resolveGapResume(lastReadTime.Time.UnixNano(), lastReadTime.Offset, earliestTime.UnixNano(), lastCheckedFlushTsNs, lastEvictedTsNs); advance {
				gapStall.resumed()
				glog.V(3).Infof("gap detected: skipping from %v to flushed earliest memory time %v for %v",
					lastReadTime.Time, earliestTime, clientName)
				lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
				readInMemoryLogErr = nil
			} else {
				reason := fmt.Sprintf("gap is not flushed yet (earliest memory %v, flushed through %v)",
					earliestTime, time.Unix(0, lastCheckedFlushTsNs))
				if skipTo, skip, done := fs.parkOnGap(ctx, req, gapStall, fs.filer.LocalMetaLogBuffer.GetLastEvictedTsNs, lastReadTime, localFlushChan, reason); done {
					return nil
				} else if skip {
					lastReadTime = log_buffer.NewMessagePosition(skipTo, gapResumeCursorOffset)
					readInMemoryLogErr = nil
				}
				continue
			}
		} else if !diskAdvanced && errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
			// The last memory read refused this cursor, the disk has nothing
			// more, yet nothing after the cursor was evicted: the cursor's
			// exclusive offset predates the retained window. Re-arm it onto
			// the window; failing even that, wait for the next flush.
			if advanceToTsNs, advance := resolveGapResume(lastReadTime.Time.UnixNano(), lastReadTime.Offset, earliestTime.UnixNano(), lastCheckedFlushTsNs, lastEvictedTsNs); advance {
				gapStall.resumed()
				lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
				readInMemoryLogErr = nil
			} else {
				reason := fmt.Sprintf("memory refuses the cursor (earliest memory %v, flushed through %v)",
					earliestTime, time.Unix(0, lastCheckedFlushTsNs))
				if skipTo, skip, done := fs.parkOnGap(ctx, req, gapStall, fs.filer.LocalMetaLogBuffer.GetLastEvictedTsNs, lastReadTime, localFlushChan, reason); done {
					return nil
				} else if skip {
					lastReadTime = log_buffer.NewMessagePosition(skipTo, gapResumeCursorOffset)
					readInMemoryLogErr = nil
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
			if errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
				// Fell behind the ring: back to the disk pass (it re-runs when
				// the flush or the cursor moved), and from there to the gap
				// resolution above if the disk has nothing either.
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
