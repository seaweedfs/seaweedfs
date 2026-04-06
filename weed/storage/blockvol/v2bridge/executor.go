package v2bridge

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Executor performs real recovery I/O using blockvol internals.
// It executes what the engine tells it to do — it does NOT decide
// recovery policy.
//
// Implements engine.CatchUpIO and engine.RebuildIO interfaces.
//
// Mode detection via rebuildAddr:
//   - rebuildAddr == "": catch-up mode. StreamWALEntries reads local WAL.
//   - rebuildAddr != "": rebuild mode. StreamWALEntries connects to primary
//     via TCP, receives entries, and applies them to the local vol.
//     TransferFullBase and TransferSnapshot also use TCP.
type Executor struct {
	vol         *blockvol.BlockVol
	rebuildAddr string // primary's rebuild server address
	replicaID   string // bounded catch-up target on the primary path
}

// NewExecutor creates an executor.
//   - vol: the blockvol instance this executor operates on.
//     For catch-up: the primary's vol (reads WAL).
//     For rebuild:  the replica's vol (receives and installs data).
//   - rebuildAddr: primary's rebuild server address.
//     Required for rebuild operations. May be empty for catch-up only.
func NewExecutor(vol *blockvol.BlockVol, rebuildAddr string, replicaID ...string) *Executor {
	exec := &Executor{vol: vol, rebuildAddr: rebuildAddr}
	if len(replicaID) > 0 {
		exec.replicaID = replicaID[0]
	}
	return exec
}

// StreamWALEntries reads WAL entries from startExclusive+1 to endInclusive.
//
// Mode depends on rebuildAddr:
//   - No rebuildAddr (catch-up mode): reads from local vol's WAL via ScanWALEntries.
//     Returns the highest LSN successfully scanned. Entries are read but not
//     applied locally (the caller ships them to the replica).
//   - With rebuildAddr (rebuild tail-replay mode): connects to the primary's
//     rebuild server via TCP, receives entries, and applies each to the local
//     vol via ApplyRebuildEntry. This is the single-executor path for
//     snapshot_tail rebuild — no test shim needed.
func (e *Executor) StreamWALEntries(startExclusive, endInclusive uint64) (uint64, error) {
	if e.vol == nil {
		return 0, fmt.Errorf("no blockvol instance")
	}

	if e.rebuildAddr != "" {
		// Rebuild tail-replay: TCP → apply to local vol.
		return e.streamAndApplyRemote(startExclusive, endInclusive)
	}
	if e.replicaID != "" {
		// Primary catch-up: replay retained WAL directly to the targeted replica
		// before live-tail shipping is allowed for this session.
		return e.vol.CatchUpReplicaTo(e.replicaID, endInclusive)
	}

	// Catch-up: local WAL scan.
	var highestLSN uint64
	err := e.vol.ScanWALEntries(startExclusive+1, func(entry *blockvol.WALEntry) error {
		if entry.LSN > endInclusive {
			return nil
		}
		highestLSN = entry.LSN
		return nil
	})
	if err != nil {
		return highestLSN, fmt.Errorf("WAL scan from %d: %w", startExclusive, err)
	}
	return highestLSN, nil
}

// streamAndApplyRemote connects to the primary's rebuild server, requests
// WAL entries from startExclusive+1, and applies them locally up to
// endInclusive. Returns the highest LSN successfully applied.
//
// Used by both TransferFullBase (second catch-up) and the snapshot_tail
// rebuild path (tail replay after snapshot install).
func (e *Executor) streamAndApplyRemote(startExclusive, endInclusive uint64) (uint64, error) {
	conn, err := net.Dial("tcp", e.rebuildAddr)
	if err != nil {
		return 0, fmt.Errorf("WAL replay connect %s: %w", e.rebuildAddr, err)
	}
	defer conn.Close()

	// Request WAL entries starting from startExclusive+1.
	// The rebuild server's handleWALCatchUp scans from FromLSN onwards.
	req := blockvol.RebuildRequest{
		Type:    blockvol.RebuildWALCatchUp,
		FromLSN: startExclusive + 1,
		Epoch:   e.vol.Epoch(),
	}
	if err := blockvol.WriteFrame(conn, blockvol.MsgRebuildReq, blockvol.EncodeRebuildRequest(req)); err != nil {
		return 0, fmt.Errorf("WAL replay send request: %w", err)
	}

	var highestLSN uint64
	var applied, skipped int
	for {
		msgType, payload, err := blockvol.ReadFrame(conn)
		if err != nil {
			return highestLSN, fmt.Errorf("WAL replay read: %w", err)
		}

		switch msgType {
		case blockvol.MsgRebuildEntry:
			// Bound to endInclusive: skip entries past the target.
			if endInclusive > 0 && len(payload) >= 8 {
				entryLSN := binary.LittleEndian.Uint64(payload[:8])
				if entryLSN > endInclusive {
					skipped++
					continue
				}
			}
			if err := e.vol.ApplyRebuildEntry(payload); err != nil {
				return highestLSN, fmt.Errorf("WAL replay apply: %w", err)
			}
			if len(payload) >= 8 {
				highestLSN = binary.LittleEndian.Uint64(payload[:8])
			}
			applied++

		case blockvol.MsgRebuildDone:
			// Sync receiver progress to the highest applied entry.
			if highestLSN > 0 {
				e.vol.SyncReceiverProgress(highestLSN)
			}
			log.Printf("v2bridge: WAL replay applied=%d skipped=%d from=%d target=%d highest=%d",
				applied, skipped, startExclusive+1, endInclusive, highestLSN)
			return highestLSN, nil

		case blockvol.MsgRebuildError:
			return highestLSN, fmt.Errorf("WAL replay server error: %s", string(payload))

		default:
			return highestLSN, fmt.Errorf("WAL replay unexpected message 0x%02x", msgType)
		}
	}
}

// TransferFullBase connects to the primary's rebuild server over TCP,
// receives the full extent image, installs it locally with full state
// handoff (clear dirty map, reset WAL, update superblock), then performs
// a second catch-up bounded to committedLSN to cover any writes that
// arrived during the copy.
//
// committedLSN is the engine's frozen minimum target (plan.RebuildTargetLSN).
// The executor validates that the server's snapshot covers this target.
//
// Returns achievedLSN: the actual boundary reached after install + second
// catch-up. achievedLSN >= committedLSN. The engine uses achievedLSN for
// progress recording so local runtime and engine-visible state converge.
func (e *Executor) TransferFullBase(committedLSN uint64) (uint64, error) {
	if e.vol == nil {
		return 0, fmt.Errorf("no blockvol instance")
	}
	if e.rebuildAddr == "" {
		return 0, fmt.Errorf("no rebuild address configured")
	}

	// Phase 1: extent copy + full state handoff.
	snapshotLSN, err := e.transferExtent()
	if err != nil {
		return 0, err
	}

	// Validate: the server's snapshot must cover the engine's frozen target.
	if committedLSN > 0 && snapshotLSN > 0 && snapshotLSN <= committedLSN {
		return 0, fmt.Errorf("rebuild: server snapshot %d does not cover target %d",
			snapshotLSN, committedLSN)
	}

	log.Printf("v2bridge: TransferFullBase phase 1 complete: extent installed, snapshotLSN=%d target=%d",
		snapshotLSN, committedLSN)

	// Phase 2: second catch-up — replay WAL entries from snapshotLSN,
	// bounded to committedLSN. Uses streamAndApplyRemote (same TCP path
	// as rebuild tail replay).
	if snapshotLSN > 0 {
		// startExclusive = snapshotLSN - 1 so FromLSN = snapshotLSN.
		_, err := e.streamAndApplyRemote(snapshotLSN-1, committedLSN)
		if err != nil {
			return 0, fmt.Errorf("rebuild second catch-up: %w", err)
		}
		log.Printf("v2bridge: TransferFullBase phase 2 complete: second catch-up snapshotLSN=%d→target=%d",
			snapshotLSN, committedLSN)
	}

	// achievedLSN: the actual boundary after all phases.
	achievedLSN := e.vol.StatusSnapshot().WALHeadLSN
	e.vol.SyncReceiverProgress(achievedLSN)

	log.Printf("v2bridge: TransferFullBase done: target=%d achieved=%d", committedLSN, achievedLSN)
	return achievedLSN, nil
}

// transferExtent connects to the rebuild server, receives the full extent,
// and installs it with full state handoff. Returns the server's snapshotLSN.
func (e *Executor) transferExtent() (snapshotLSN uint64, err error) {
	conn, err := net.Dial("tcp", e.rebuildAddr)
	if err != nil {
		return 0, fmt.Errorf("rebuild connect %s: %w", e.rebuildAddr, err)
	}
	defer conn.Close()

	req := blockvol.RebuildRequest{
		Type:  blockvol.RebuildFullExtent,
		Epoch: e.vol.Epoch(),
	}
	if err := blockvol.WriteFrame(conn, blockvol.MsgRebuildReq, blockvol.EncodeRebuildRequest(req)); err != nil {
		return 0, fmt.Errorf("rebuild send request: %w", err)
	}

	installer := e.vol.NewRebuildInstaller()

	for {
		msgType, payload, err := blockvol.ReadFrame(conn)
		if err != nil {
			return 0, fmt.Errorf("rebuild read frame: %w", err)
		}

		switch msgType {
		case blockvol.MsgRebuildExtent:
			if err := installer.WriteChunk(payload); err != nil {
				return 0, fmt.Errorf("rebuild install chunk: %w", err)
			}

		case blockvol.MsgRebuildDone:
			if len(payload) >= 8 {
				snapshotLSN = binary.BigEndian.Uint64(payload[:8])
			}
			if err := installer.Commit(snapshotLSN); err != nil {
				return 0, fmt.Errorf("rebuild install commit: %w", err)
			}
			log.Printf("v2bridge: extent installed: %d bytes, snapshotLSN=%d from %s",
				installer.BytesWritten(), snapshotLSN, e.rebuildAddr)
			return snapshotLSN, nil

		case blockvol.MsgRebuildError:
			return 0, fmt.Errorf("rebuild server error: %s", string(payload))

		default:
			return 0, fmt.Errorf("rebuild unexpected message 0x%02x", msgType)
		}
	}
}

// TransferSnapshot connects to the primary's rebuild server, requests an
// exact snapshot export at snapshotLSN, streams the image directly to disk
// (no memory buffering), verifies SHA-256, and converges all local runtime
// state to snapshotLSN.
//
// Unlike TransferFullBase (conservative >= target), TransferSnapshot
// requires an EXACT boundary. This is enforced at three levels:
//   - server: verifies checkpoint == requestedLSN before export
//   - manifest: carries explicit BaseLSN
//   - client: verifies manifest.BaseLSN == snapshotLSN before commit
//
// On partial failure (mid-stream disconnect, write error), the extent
// may contain mixed data. This is the same limitation as P1 full-base
// and V1 rebuild: on failure, the engine does not complete the rebuild,
// and master re-issues the assignment for a fresh attempt.
func (e *Executor) TransferSnapshot(snapshotLSN uint64) error {
	if e.vol == nil {
		return fmt.Errorf("no blockvol instance")
	}
	if e.rebuildAddr == "" {
		return fmt.Errorf("no rebuild address configured")
	}

	conn, err := net.Dial("tcp", e.rebuildAddr)
	if err != nil {
		return fmt.Errorf("snapshot connect %s: %w", e.rebuildAddr, err)
	}
	defer conn.Close()

	req := blockvol.RebuildRequest{
		Type:    blockvol.RebuildSnapshot,
		FromLSN: snapshotLSN,
		Epoch:   e.vol.Epoch(),
	}
	if err := blockvol.WriteFrame(conn, blockvol.MsgRebuildReq, blockvol.EncodeRebuildRequest(req)); err != nil {
		return fmt.Errorf("snapshot send request: %w", err)
	}

	// Stream snapshot image directly to disk via RebuildInstaller.
	// Compute SHA-256 inline — no memory buffering of the full image.
	installer := e.vol.NewRebuildInstaller()
	hash := sha256.New()
	var manifestJSON []byte
	var serverBaseLSN uint64

	for {
		msgType, payload, err := blockvol.ReadFrame(conn)
		if err != nil {
			return fmt.Errorf("snapshot read frame: %w", err)
		}

		switch msgType {
		case blockvol.MsgRebuildExtent:
			// Write chunk to extent AND hash inline.
			hash.Write(payload)
			if err := installer.WriteChunk(payload); err != nil {
				return fmt.Errorf("snapshot install chunk: %w", err)
			}

		case blockvol.MsgRebuildEntry:
			// Manifest (JSON). Sent after all extent chunks.
			manifestJSON = payload

		case blockvol.MsgRebuildDone:
			if len(payload) >= 8 {
				serverBaseLSN = binary.BigEndian.Uint64(payload[:8])
			}
			goto transferComplete

		case blockvol.MsgRebuildError:
			return fmt.Errorf("snapshot server error: %s", string(payload))

		default:
			return fmt.Errorf("snapshot unexpected message 0x%02x", msgType)
		}
	}

transferComplete:
	// Validate server boundary.
	if serverBaseLSN != snapshotLSN {
		return fmt.Errorf("snapshot boundary mismatch: server=%d requested=%d",
			serverBaseLSN, snapshotLSN)
	}

	// Parse and validate manifest.
	if len(manifestJSON) == 0 {
		return fmt.Errorf("snapshot: no manifest received")
	}
	manifest, err := blockvol.UnmarshalManifest(manifestJSON)
	if err != nil {
		return fmt.Errorf("snapshot manifest: %w", err)
	}
	if manifest.BaseLSN != snapshotLSN {
		return fmt.Errorf("snapshot manifest BaseLSN=%d != requested %d",
			manifest.BaseLSN, snapshotLSN)
	}

	// Verify SHA-256 (computed inline during streaming).
	gotHash := hex.EncodeToString(hash.Sum(nil))
	if gotHash != manifest.SHA256 {
		return fmt.Errorf("snapshot checksum mismatch: got %s, want %s", gotHash, manifest.SHA256)
	}

	// Commit: state handoff with exact snapshot boundary.
	// snapshotLSN IS the last entry (BaseLSN). Pass snapshotLSN+1 to Commit
	// so nextLSN = snapshotLSN+1 and checkpointLSN = snapshotLSN.
	if err := installer.Commit(snapshotLSN + 1); err != nil {
		return fmt.Errorf("snapshot install commit: %w", err)
	}

	log.Printf("v2bridge: TransferSnapshot installed: %d bytes, BaseLSN=%d, SHA256 verified",
		installer.BytesWritten(), snapshotLSN)
	return nil
}

// TruncateWAL performs real local correction for replica-ahead recovery.
//
// Detection rule: truncation is safe only when the kept base boundary already
// matches the local checkpoint. This is determined inside `TruncateToLSN`
// after the flusher is paused and I/O is drained:
//   - CheckpointLSN == truncateLSN: safe — extent has the exact kept base,
//     and ahead entries exist only above that boundary.
//   - CheckpointLSN != truncateLSN: unsafe — either ahead entries already
//     contaminated extent (`>`) or part of the kept range still exists only
//     in WAL (`<`). Returns an error so the engine escalates to rebuild.
//
// On success (truncation-safe case): delegates to blockvol.TruncateToLSN
// which pauses the flusher, clears dirty map, resets WAL, and converges
// all runtime state to exactly truncateLSN.
func (e *Executor) TruncateWAL(truncateLSN uint64) error {
	if e.vol == nil {
		return fmt.Errorf("no blockvol instance")
	}

	if err := e.vol.TruncateToLSN(truncateLSN); err != nil {
		// If blockvol reports truncation unsafe, wrap with the engine's
		// sentinel so CatchUpExecutor can detect and escalate to rebuild.
		if errors.Is(err, blockvol.ErrTruncationUnsafe) {
			return fmt.Errorf("%w: %v", engine.ErrTruncationUnsafe, err)
		}
		return fmt.Errorf("truncate WAL to %d: %w", truncateLSN, err)
	}
	log.Printf("v2bridge: TruncateWAL complete: truncateLSN=%d", truncateLSN)
	return nil
}
