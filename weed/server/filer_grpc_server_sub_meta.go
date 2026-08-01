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

	// maxGapStall bounds a gap wait before giving up and skipping it, counted
	// and logged: a dead peer makes the wait permanent, and failing the stream
	// only moves the loop into a client that reconnects to the same wall.
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

// reportUnprovenAggregatedCrossing records the residual hole: a disk read that
// crosses the eviction watermark may have advanced on one peer's log while a
// lagging peer still holds unflushed events in the crossed range. Locally
// undecidable (log files carry random filer ids, peers are tracked by address);
// closing it needs each peer's flush watermark on the subscribe stream.
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

// gapResumeCursorOffset marks every cursor these loops hand to the memory read:
// gated, so a seal racing the loop's watermark check is refused under the
// read's own lock instead of silently served from the earliest window.
const gapResumeCursorOffset = log_buffer.EvictionGatedOffset

// memoryHoldsGap reports whether nothing after the cursor was evicted. Equality
// counts: the evicted window ends on the watermark, retained windows start
// strictly after it, and the persisted reader skips ts <= cursor, so no wait
// can ever produce the boundary entry - refusing there never ends.
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
	// Done exits run before park(): a finished stream was never parked, and
	// marking it so leaves a false "still behind" trace. A cursor at UntilNs is
	// finished - the bound is inclusive, cursors are exclusive, and
	// LoopProcessLogData (the only place UntilNs ends a stream) is unreachable
	// from a park.
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

// gapPass carries what the shared post-disk gap decisions differ by between
// the two subscribe loops; everything else about them must stay identical, and
// this PR's history shows they drift when edited separately.
type gapPass struct {
	fs        *FilerServer
	req       *filer_pb.SubscribeMetadataRequest
	gapStall  *gapStallReporter
	earliest  func() time.Time
	evicted   func() int64 // gap-proof watermark; aggregated uses the received-ts space
	flushed   func() int64 // flush watermark the last disk read observed; aggregated: 0
	gapChan   <-chan struct{}
	dataChan  <-chan struct{}
	gapReason func(earliest time.Time, evictedTsNs int64) string
}

type gapOutcome int

const (
	gapProceed  gapOutcome = iota // read memory
	gapContinue                   // restart the pass
	gapDone                       // the stream is over
)

// resolve is the gap decision both loops run between the disk pass and the
// memory read. A cursor the ring evicted past cannot be served from memory
// without skipping what was dropped: keep draining the disk if it just moved,
// skip if a proof says the gap is empty, park otherwise. A cursor memory
// refused with nothing evicted after it re-arms onto the retained window.
func (p *gapPass) resolve(ctx context.Context, cursor *log_buffer.MessagePosition, latch *error, diskAdvanced bool) gapOutcome {
	earliest := p.earliest()
	evictedTsNs := p.evicted()
	cursorTsNs := cursor.Time.UnixNano()
	if !memoryHoldsGap(cursorTsNs, evictedTsNs) {
		if diskAdvanced {
			return gapContinue // the disk may hold more of the gap
		}
		if advanceToTsNs, advance := resolveGapResume(cursorTsNs, cursor.Offset, earliest.UnixNano(), p.flushed(), evictedTsNs); advance {
			p.gapStall.resumed()
			glog.V(3).Infof("%s subscriber %s: gap proven empty, skipping from %v to earliest memory %v",
				p.gapStall.scope, p.gapStall.clientName, cursor.Time, earliest)
			*cursor = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
			*latch = nil
			return gapProceed
		}
		return p.park(ctx, cursor, latch, p.gapChan, p.gapReason(earliest, evictedTsNs))
	}
	if !diskAdvanced && errors.Is(*latch, log_buffer.ResumeFromDiskError) {
		// Memory refused the cursor though nothing after it was evicted: its
		// exclusive offset predates the retained window. Re-arm it onto the
		// window; failing even that, wait for data.
		if advanceToTsNs, advance := resolveGapResume(cursorTsNs, cursor.Offset, earliest.UnixNano(), p.flushed(), evictedTsNs); advance {
			p.gapStall.resumed()
			*cursor = log_buffer.NewMessagePosition(advanceToTsNs, gapResumeCursorOffset)
			*latch = nil
			return gapProceed
		}
		return p.park(ctx, cursor, latch, p.dataChan, "no readable in-memory entries yet")
	}
	return gapProceed
}

func (p *gapPass) park(ctx context.Context, cursor *log_buffer.MessagePosition, latch *error, notifyChan <-chan struct{}, reason string) gapOutcome {
	skipTo, skip, done := p.fs.parkOnGap(ctx, p.req, p.gapStall, p.evicted, *cursor, notifyChan, reason)
	if done {
		return gapDone
	}
	if skip {
		*cursor = log_buffer.NewMessagePosition(skipTo, gapResumeCursorOffset)
		*latch = nil
	}
	return gapContinue
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

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, gapResumeCursorOffset)
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
	sentRefs := make(map[string]sentRefState)

	aggBuffer := fs.filer.MetaAggregator.MetaLogBuffer
	gaps := &gapPass{
		fs:       fs,
		req:      req,
		gapStall: gapStall,
		earliest: aggBuffer.GetEarliestTime,
		evicted:  aggBuffer.GetLastEvictedOriginalTsNs,
		flushed:  func() int64 { return 0 }, // the aggregated ring never flushes
		gapChan:  nil,                       // nothing local signals a peer's flush; the timer paces it
		dataChan: aggNotifyChan,
		gapReason: func(earliest time.Time, evictedTsNs int64) string {
			return fmt.Sprintf("gap evicted through %v is not on a peer's disk yet (earliest memory %v)",
				time.Unix(0, evictedTsNs), earliest)
		},
	}

	for {

		glog.V(4).Infof("read on disk %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		cursorBeforeDiskTsNs := lastReadTime.Time.UnixNano()

		if req.ClientSupportsMetadataChunks {
			processedTsNs, isDone, readPersistedLogErr = fs.chunkDiskPass(ctx, sender, lastReadTime, req.UntilNs, &refsSentAtTsNs, sentRefs)
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
		// Read after the disk read (an eviction landing mid-read must count) and
		// in received-ts space: the ring's bumped stopTimes exceed anything on
		// any peer's disk, and gating disk cursors on them parks subscribers
		// that drained every peer's log.
		lastEvictedTsNs := fs.filer.MetaAggregator.MetaLogBuffer.GetLastEvictedOriginalTsNs()
		if diskAdvanced {
			gapStall.resumed()
			reportUnprovenAggregatedCrossing(cursorBeforeDiskTsNs, processedTsNs, lastEvictedTsNs, clientName, req.PathPrefix)
			lastReadTime = log_buffer.NewMessagePosition(processedTsNs, gapResumeCursorOffset)
		} else if readInMemoryLogErr == nil {
			// Nothing on disk and memory never spoke: scan forward for the next
			// day that has logs.
			nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
			position := log_buffer.NewMessagePosition(nextDayTs, gapResumeCursorOffset)
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

		switch gaps.resolve(ctx, &lastReadTime, &readInMemoryLogErr, diskAdvanced) {
		case gapDone:
			return nil
		case gapContinue:
			continue
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

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, gapResumeCursorOffset)
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
	sentRefs := make(map[string]sentRefState)

	localBuffer := fs.filer.LocalMetaLogBuffer
	gaps := &gapPass{
		fs:       fs,
		req:      req,
		gapStall: gapStall,
		earliest: localBuffer.GetEarliestTime,
		evicted:  localBuffer.GetLastEvictedTsNs, // local disk carries the ring's own timestamps
		flushed:  func() int64 { return lastCheckedFlushTsNs },
		gapChan:  localFlushChan,
		dataChan: localFlushChan,
		gapReason: func(earliest time.Time, evictedTsNs int64) string {
			return fmt.Sprintf("gap is not flushed yet (earliest memory %v, flushed through %v)",
				earliest, time.Unix(0, lastCheckedFlushTsNs))
		},
	}

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
			if req.ClientSupportsMetadataChunks {
				processedTsNs, isDone, readPersistedLogErr = fs.chunkDiskPass(ctx, sender, lastReadTime, req.UntilNs, &refsSentAtTsNs, sentRefs)
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
				lastReadTime = log_buffer.NewMessagePosition(processedTsNs, gapResumeCursorOffset)
			} else if readInMemoryLogErr == nil {
				// Nothing on disk and memory never spoke: scan forward for the
				// next day that has logs.
				nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
				position := log_buffer.NewMessagePosition(nextDayTs, gapResumeCursorOffset)
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

		switch gaps.resolve(ctx, &lastReadTime, &readInMemoryLogErr, diskAdvanced) {
		case gapDone:
			return nil
		case gapContinue:
			continue
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

// chunkDiskPass is the disk step for chunk-capable clients: ship the unsent
// refs, advance the cursor by an entry read that delivers nothing (refs cover
// the content), re-ship anything that entry read saw beyond the first delta,
// then send a transition. The transition is an empty-notification marker: both
// chunk consumers buffer refs until a non-ref message, so an idle source -
// historical logs, empty ring - would otherwise strand the backlog in the
// client's pending list until the next mutation. Its timestamp becomes the
// client's resume filter, which is why the second delta must run first: the
// entry read can outrun the first delta by a chunk appended in between, and a
// filter past unshipped content silently drops it.
func (fs *FilerServer) chunkDiskPass(ctx context.Context, sender metadataStreamSender, startPos log_buffer.MessagePosition, untilNs int64, refsSentAtTsNs *int64, sent map[string]sentRefState) (processedTsNs int64, isDone bool, err error) {
	cursor := startPos
	refsSent := false
	finish := func(done bool) (int64, bool, error) {
		if refsSent {
			if sendErr := sender.Send(&filer_pb.SubscribeMetadataResponse{
				EventNotification: &filer_pb.EventNotification{},
				TsNs:              cursor.Time.UnixNano(),
			}); sendErr != nil {
				return 0, false, sendErr
			}
		}
		return cursor.Time.UnixNano(), done, nil
	}
	if cursor.Time.UnixNano() != *refsSentAtTsNs {
		*refsSentAtTsNs = cursor.Time.UnixNano()
		refsTsNs, sentAny, done, refsErr := fs.sendLogFileRefs(ctx, sender, cursor, untilNs, sent)
		if refsErr != nil {
			return 0, false, refsErr
		}
		refsSent = sentAny
		if diskReadAdvanced(refsTsNs, cursor) {
			cursor = log_buffer.NewMessagePosition(refsTsNs, gapResumeCursorOffset)
		}
		if done {
			return finish(true)
		}
	}
	noop := func(*filer_pb.LogEntry) (bool, error) { return false, nil }
	entryTsNs, done, entryErr := fs.filer.ReadPersistedLogBuffer(ctx, cursor, untilNs, noop)
	if entryErr != nil {
		return 0, false, entryErr
	}
	if diskReadAdvanced(entryTsNs, cursor) {
		cursor = log_buffer.NewMessagePosition(entryTsNs, gapResumeCursorOffset)
		*refsSentAtTsNs = entryTsNs
		_, sentAny, done2, refsErr := fs.sendLogFileRefs(ctx, sender, cursor, untilNs, sent)
		if refsErr != nil {
			return 0, false, refsErr
		}
		refsSent = refsSent || sentAny
		done = done || done2
	}
	return finish(done)
}

// sentRefState tracks, per subscription, how many chunks of each log file have
// been shipped as refs. Collection re-lists files up to a flush interval behind
// the cursor (the spanning-file back-off), and a filer appends further chunks
// to its newest file, so consecutive collections overlap; shipping only each
// file's unsent chunk suffix keeps every per-filer ref stream duplicate-free
// and timestamp-sorted - the contract the client's merge reads them under.
type sentRefState struct {
	chunks   int
	fileTsNs int64
}

// deltaLogFileRefs reduces a collection to the chunks not yet shipped, updates
// the sent state, and prunes files the scan window has moved past.
//
// A shipped suffix is rebased to logical offset zero: the client's chunk
// reader starts at zero, and a chunk list opening at a higher offset reads as
// instant EOF - an empty replay that would silently drop the appended events.
// The cut is record-aligned because each append is one chunk of whole entries
// (logFlushFunc appends one uploaded window per flush), so the rebased suffix
// decodes as a file of its own.
func deltaLogFileRefs(refs []*filer_pb.LogFileChunkRef, sent map[string]sentRefState, pruneBeforeTsNs int64) []*filer_pb.LogFileChunkRef {
	out := make([]*filer_pb.LogFileChunkRef, 0, len(refs))
	for _, ref := range refs {
		key := fmt.Sprintf("%s/%d", ref.FilerId, ref.FileTsNs)
		prior := sent[key].chunks
		if len(ref.Chunks) <= prior {
			continue
		}
		chunks := ref.Chunks[prior:]
		if base := chunks[0].Offset; base != 0 {
			rebased := make([]*filer_pb.FileChunk, len(chunks))
			for i, c := range chunks {
				cc := proto.Clone(c).(*filer_pb.FileChunk)
				cc.Offset -= base
				rebased[i] = cc
			}
			chunks = rebased
		}
		out = append(out, &filer_pb.LogFileChunkRef{
			Chunks:   chunks,
			FileTsNs: ref.FileTsNs,
			FilerId:  ref.FilerId,
		})
		sent[key] = sentRefState{chunks: len(ref.Chunks), fileTsNs: ref.FileTsNs}
	}
	for key, st := range sent {
		if st.fileTsNs < pruneBeforeTsNs {
			delete(sent, key)
		}
	}
	return out
}

// sendLogFileRefs collects persisted log file chunk references and sends the
// ones this subscription has not shipped yet, so the client can read the data
// directly from volume servers. Zero volume server I/O here — only filer store
// listings. Refs go through the pipelinedSender like every other message: gRPC
// allows one sending goroutine per stream, and the sender's goroutine is it;
// the sender keeps refs messages out of Events batches.
func (fs *FilerServer) sendLogFileRefs(ctx context.Context, stream metadataStreamSender, startPosition log_buffer.MessagePosition, stopTsNs int64, sent map[string]sentRefState) (lastTsNs int64, sentAny bool, isDone bool, err error) {
	collected, lastTsNs, err := fs.filer.CollectLogFileRefs(ctx, startPosition, stopTsNs)
	if err != nil {
		return 0, false, false, err
	}
	refs := deltaLogFileRefs(collected, sent, filer.PersistedLogScanStartTsNs(startPosition.Time))
	if len(refs) == 0 {
		return lastTsNs, false, false, nil
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
			return lastTsNs, true, false, err
		}
	}
	return lastTsNs, true, false, nil
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
