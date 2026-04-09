package blockvol

import (
	"fmt"
	"sync"
)

// RebuildSessionPhase tracks the lifecycle of one rebuild session on the
// replica side. Matches the replica state machine in v2-rebuild-mvp-session-protocol.md.
type RebuildSessionPhase string

const (
	RebuildPhaseIdle         RebuildSessionPhase = "idle"
	RebuildPhaseAccepted     RebuildSessionPhase = "accepted"
	RebuildPhaseRunning      RebuildSessionPhase = "running"
	RebuildPhaseBaseComplete RebuildSessionPhase = "base_complete"
	RebuildPhaseCompleted    RebuildSessionPhase = "completed"
	RebuildPhaseFailed       RebuildSessionPhase = "failed"
)

// RebuildSessionConfig is the contract for starting one rebuild session.
// Issued by the primary via sessionControl(start_rebuild).
type RebuildSessionConfig struct {
	SessionID uint64
	Epoch     uint64
	BaseLSN   uint64 // flushed/checkpoint boundary — base stream anchored at or above this LSN
	TargetLSN uint64 // WAL must reach this before completion
}

// RebuildSession manages one replica-side rebuild session with two concurrent
// data lanes:
//
//   - Base lane: extent blocks (anchored at or above BaseLSN) with bitmap protection
//   - WAL lane: live WAL entries applied and marked in bitmap
//
// The bitmap ensures WAL-applied data always wins over base data. The session
// completes when both base is fully transferred AND WAL has reached the target.
//
// This is session-scoped volatile state. After crash, the session must restart
// from scratch. Durable WAL entries survive via local WAL replay.
type RebuildSession struct {
	mu     sync.Mutex
	config RebuildSessionConfig
	phase  RebuildSessionPhase
	bitmap *RebuildBitmap
	vol    *BlockVol

	// Progress tracking
	walAppliedLSN     uint64 // highest WAL LSN applied during this session
	baseBlocksTotal   uint64 // total base blocks to transfer
	baseBlocksApplied uint64 // base blocks successfully applied (not skipped)
	baseBlocksSkipped uint64 // base blocks skipped due to bitmap conflict
	baseComplete      bool   // all base blocks have been processed
	failReason        string
}

// NewRebuildSession creates a replica-side rebuild session. The session starts
// in Accepted phase. Call Start() to transition to Running.
func NewRebuildSession(vol *BlockVol, config RebuildSessionConfig) (*RebuildSession, error) {
	if vol == nil {
		return nil, fmt.Errorf("rebuild session: volume is nil")
	}
	if config.TargetLSN == 0 {
		return nil, fmt.Errorf("rebuild session: target LSN is required")
	}
	info := vol.Info()
	totalLBAs := info.VolumeSize / uint64(info.BlockSize)
	bitmap := NewRebuildBitmap(totalLBAs, info.BlockSize)

	session := &RebuildSession{
		config: config,
		phase:  RebuildPhaseAccepted,
		bitmap: bitmap,
		vol:    vol,
		// The trusted base already covers BaseLSN. Hydration may advance this
		// further if recovered local WAL survives past the base boundary.
		walAppliedLSN: config.BaseLSN,
	}
	// Full-base rebuild intentionally replaces the replica's local runtime
	// state from scratch, so stale local WAL/dirty-map entries must not be
	// hydrated into the session. Two-line sessions with a genuine WAL lane
	// still hydrate recovered WAL newer than BaseLSN before base intake opens.
	if config.TargetLSN != config.BaseLSN {
		if err := session.hydrateBitmapFromRecoveredWAL(); err != nil {
			return nil, err
		}
	}
	return session, nil
}

// Start transitions the session from Accepted to Running.
func (s *RebuildSession) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != RebuildPhaseAccepted {
		return fmt.Errorf("rebuild session: cannot start from phase %s", s.phase)
	}
	s.phase = RebuildPhaseRunning
	return nil
}

func (s *RebuildSession) hydrateBitmapFromRecoveredWAL() error {
	if s == nil || s.vol == nil {
		return fmt.Errorf("rebuild session: volume is nil")
	}
	// Fail closed if the replica's durable extent is already newer than the
	// incoming base. Those overwritten ranges can no longer be reconstructed
	// from retained WAL alone, so a fresh older base is unsafe.
	if s.vol.super.WALCheckpointLSN > s.config.BaseLSN {
		return fmt.Errorf("rebuild session: local checkpoint %d is newer than base LSN %d",
			s.vol.super.WALCheckpointLSN, s.config.BaseLSN)
	}
	if s.vol.dirtyMap == nil {
		return nil
	}
	for _, entry := range s.vol.dirtyMap.Snapshot() {
		if entry.Lsn <= s.config.BaseLSN {
			continue
		}
		s.markBitmapRange(entry.Lba, entry.Length)
		if entry.Lsn > s.walAppliedLSN {
			s.walAppliedLSN = entry.Lsn
		}
	}
	return nil
}

// ApplyWALEntry applies one WAL entry through the WAL lane. The entry is
// applied to the replica's local WAL, and the bitmap bit is set for each
// LBA covered by the entry. This ensures base lane data for the same LBA
// will be skipped (WAL always wins).
//
// The bitmap bit is set AFTER successful WAL append (applied), not on
// receive. This is the key correctness invariant.
func (s *RebuildSession) ApplyWALEntry(entry *WALEntry) error {
	s.mu.Lock()
	if s.phase != RebuildPhaseRunning && s.phase != RebuildPhaseBaseComplete {
		s.mu.Unlock()
		return fmt.Errorf("rebuild session: WAL apply not allowed in phase %s", s.phase)
	}
	if entry.Epoch != s.config.Epoch {
		s.mu.Unlock()
		return fmt.Errorf("rebuild session: epoch mismatch: entry=%d session=%d", entry.Epoch, s.config.Epoch)
	}

	// Apply to local WAL via the volume's WAL writer.
	if err := s.vol.applyRebuildWALEntry(entry); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("rebuild session: WAL apply LSN=%d: %w", entry.LSN, err)
	}

	// AFTER successful apply: mark bitmap for each covered LBA. Writes and
	// trims both define state newer than the trusted base, so the base lane
	// must never overwrite them after local apply succeeds.
	if (entry.Type == EntryTypeWrite || entry.Type == EntryTypeTrim) && entry.Length > 0 {
		s.markBitmapRange(entry.LBA, entry.Length)
	}

	if entry.LSN > s.walAppliedLSN {
		s.walAppliedLSN = entry.LSN
	}
	ack := s.sessionAckLocked()
	s.mu.Unlock()
	s.vol.emitRebuildSessionAck(ack)
	return nil
}

// ApplyBaseBlock applies one base (snapshot) block through the base lane.
// If the bitmap shows the LBA was already covered by a WAL entry, the base
// block is skipped (WAL always wins over older base data).
//
// Returns (applied bool, err error). applied=false means the block was
// skipped due to bitmap conflict, which is correct behavior.
func (s *RebuildSession) ApplyBaseBlock(lba uint64, data []byte) (bool, error) {
	s.mu.Lock()
	if s.phase != RebuildPhaseRunning {
		s.mu.Unlock()
		return false, fmt.Errorf("rebuild session: base apply not allowed in phase %s", s.phase)
	}

	// Bitmap conflict check: WAL-applied LBA wins.
	if !s.bitmap.ShouldApplyBase(lba) {
		s.baseBlocksSkipped++
		s.mu.Unlock()
		return false, nil
	}
	fullBase := s.config.TargetLSN == s.config.BaseLSN
	baseLSN := s.config.BaseLSN
	s.mu.Unlock()

	// Full-base sessions overwrite the extent unconditionally and discard prior
	// replica-local runtime state after the base lane succeeds. Two-line
	// sessions that truly race with WAL still honor the newer-than-BaseLSN
	// dirty-map guard under ioMu.
	var err error
	if fullBase {
		err = s.vol.writeExtentDirectUnconditional(lba, data)
	} else {
		err = s.vol.writeExtentDirectForRebuild(lba, data, baseLSN)
	}
	if err != nil {
		return false, fmt.Errorf("rebuild session: base apply LBA=%d: %w", lba, err)
	}

	s.mu.Lock()
	s.baseBlocksApplied++
	s.mu.Unlock()
	return true, nil
}

// MarkBaseComplete marks the base lane as fully transferred.
// The session transitions to BaseComplete phase if currently Running.
func (s *RebuildSession) MarkBaseComplete(totalBlocks uint64) {
	s.mu.Lock()
	s.baseBlocksTotal = totalBlocks
	s.baseComplete = true
	if s.phase == RebuildPhaseRunning {
		s.phase = RebuildPhaseBaseComplete
	}
	// When BaseLSN == TargetLSN, the base image covers all data — no WAL
	// tail needed. Auto-satisfy the WAL condition so TryComplete succeeds
	// immediately after base transfer.
	if s.config.BaseLSN == s.config.TargetLSN && s.walAppliedLSN < s.config.TargetLSN {
		s.walAppliedLSN = s.config.TargetLSN
	}
	ack := s.sessionAckLocked()
	s.mu.Unlock()
	s.vol.emitRebuildSessionAck(ack)
	// Try to complete immediately — covers the BaseLSN == TargetLSN case
	// where no WAL entries will arrive.
	s.TryComplete()
}

// TryComplete checks if both completion conditions are met:
//  1. base_complete = true
//  2. wal_applied_lsn >= target_lsn
//
// If both are true, transitions to Completed phase and returns the achieved LSN.
// If not ready, returns (0, false).
func (s *RebuildSession) TryComplete() (uint64, bool) {
	s.mu.Lock()
	if !s.baseComplete {
		s.mu.Unlock()
		return 0, false
	}
	if s.walAppliedLSN < s.config.TargetLSN {
		s.mu.Unlock()
		return 0, false
	}
	if s.phase == RebuildPhaseCompleted || s.phase == RebuildPhaseFailed {
		s.mu.Unlock()
		return 0, false
	}
	s.phase = RebuildPhaseCompleted
	achieved := s.walAppliedLSN
	ack := s.sessionAckLocked()
	s.mu.Unlock()
	s.vol.emitRebuildSessionAck(ack)
	return achieved, true
}

func (s *RebuildSession) ObserveAppliedLSN(appliedLSN uint64) SessionAckMsg {
	s.mu.Lock()
	if appliedLSN > s.walAppliedLSN {
		s.walAppliedLSN = appliedLSN
	}
	ack := s.sessionAckLocked()
	s.mu.Unlock()
	return ack
}

// Fail marks the session as failed with a reason.
func (s *RebuildSession) Fail(reason string) {
	s.mu.Lock()
	s.phase = RebuildPhaseFailed
	s.failReason = reason
	ack := s.sessionAckLocked()
	s.mu.Unlock()
	s.vol.emitRebuildSessionAck(ack)
}

// Phase returns the current session phase.
func (s *RebuildSession) Phase() RebuildSessionPhase {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phase
}

// WALAppliedLSN returns the highest WAL LSN applied during this session.
func (s *RebuildSession) WALAppliedLSN() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.walAppliedLSN
}

// Progress returns the current session progress for sessionAck reporting.
func (s *RebuildSession) Progress() RebuildSessionProgress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return RebuildSessionProgress{
		Phase:              s.phase,
		WALAppliedLSN:      s.walAppliedLSN,
		BaseBlocksTotal:    s.baseBlocksTotal,
		BaseBlocksApplied:  s.baseBlocksApplied,
		BaseBlocksSkipped:  s.baseBlocksSkipped,
		BaseComplete:       s.baseComplete,
		BitmapAppliedCount: s.bitmap.AppliedCount(),
		FailReason:         s.failReason,
	}
}

// Config returns the session configuration.
func (s *RebuildSession) Config() RebuildSessionConfig {
	return s.config
}

// RebuildSessionProgress is the read-only progress snapshot for sessionAck.
type RebuildSessionProgress struct {
	Phase              RebuildSessionPhase
	WALAppliedLSN      uint64
	BaseBlocksTotal    uint64
	BaseBlocksApplied  uint64
	BaseBlocksSkipped  uint64
	BaseComplete       bool
	BitmapAppliedCount uint64
	FailReason         string
}

func (p RebuildSessionProgress) Completed() bool {
	return p.Phase == RebuildPhaseCompleted
}

// blockSize returns the block size from config, defaulting to 4096.
func (c RebuildSessionConfig) blockSize() uint32 {
	// Config doesn't carry block size directly; callers use vol.Info().BlockSize.
	return 0
}

func (s *RebuildSession) SessionID() uint64 {
	return s.config.SessionID
}

func (s *RebuildSession) sessionAckLocked() SessionAckMsg {
	ack := SessionAckMsg{
		Epoch:         s.config.Epoch,
		SessionID:     s.config.SessionID,
		WALAppliedLSN: s.walAppliedLSN,
		BaseComplete:  s.baseComplete,
	}
	switch s.phase {
	case RebuildPhaseAccepted:
		ack.Phase = SessionAckAccepted
	case RebuildPhaseRunning:
		ack.Phase = SessionAckRunning
	case RebuildPhaseBaseComplete:
		ack.Phase = SessionAckBaseComplete
	case RebuildPhaseCompleted:
		ack.Phase = SessionAckCompleted
		ack.AchievedLSN = s.walAppliedLSN
	case RebuildPhaseFailed:
		ack.Phase = SessionAckFailed
	default:
		ack.Phase = SessionAckRunning
	}
	return ack
}

func (s *RebuildSession) markBitmapRange(startLBA uint64, length uint32) {
	blockSize := uint64(s.config.blockSize())
	if blockSize == 0 {
		blockSize = uint64(s.vol.Info().BlockSize)
	}
	blocks := uint64(length) / blockSize
	if blocks == 0 {
		blocks = 1
	}
	for i := uint64(0); i < blocks; i++ {
		s.bitmap.MarkApplied(startLBA + i)
	}
}

// StartRebuildSession installs and starts one active rebuild session on the
// replica. A new session supersedes any previous active rebuild session.
func (v *BlockVol) StartRebuildSession(config RebuildSessionConfig) error {
	if config.SessionID == 0 {
		return fmt.Errorf("rebuild session: session ID is required")
	}
	// Strict ordering barrier: freeze local flush/mutation briefly so the bitmap
	// hydration sees one stable view of recovered WAL coverage before the session
	// becomes visible to the base or WAL lanes.
	if v.flusher != nil {
		v.flusher.Pause()
		defer v.flusher.Resume()
	}
	v.ioMu.Lock()
	defer v.ioMu.Unlock()

	// Safety check first: NewRebuildSession's hydration will fail-closed if
	// local checkpoint is newer than baseLSN. This must run BEFORE cleanup.
	session, err := NewRebuildSession(v, config)
	if err != nil {
		return err
	}
	// Clear stale local runtime state so base blocks written directly to the
	// extent are visible via ReadLBA. Without this, old WAL replay entries
	// or stale extent data from a previous lifecycle shadow the rebuild's
	// new base data. This runs AFTER the hydration safety check but BEFORE
	// the session accepts any data.
	if v.dirtyMap != nil {
		v.dirtyMap.Clear()
	}
	if v.wal != nil {
		v.wal.Reset()
	}
	v.mu.Lock()
	v.super.WALHead = 0
	v.super.WALTail = 0
	v.super.WALCheckpointLSN = config.BaseLSN
	v.mu.Unlock()
	if v.flusher != nil {
		v.flusher.SetCheckpointLSN(config.BaseLSN)
	}
	v.nextLSN.Store(config.BaseLSN + 1)

	if err := session.Start(); err != nil {
		return err
	}

	v.rebuildSessMu.Lock()
	defer v.rebuildSessMu.Unlock()
	if v.rebuildSess != nil {
		v.rebuildSess.Fail("superseded")
	}
	v.rebuildSess = session
	// Rebuild's trusted base covers BaseLSN and hydration may discover a more
	// recent locally recoverable boundary. Let live shipping resume strictly
	// after the best known boundary.
	progressLSN := config.BaseLSN
	if hydrated := session.WALAppliedLSN(); hydrated > progressLSN {
		progressLSN = hydrated
	}
	if progressLSN > 0 {
		v.SyncReceiverProgress(progressLSN)
	}
	v.emitRebuildSessionAck(SessionAckMsg{
		Epoch:         config.Epoch,
		SessionID:     config.SessionID,
		Phase:         SessionAckAccepted,
		WALAppliedLSN: progressLSN,
		BaseComplete:  false,
	})
	return nil
}

// CancelRebuildSession cancels and removes one active rebuild session.
func (v *BlockVol) CancelRebuildSession(sessionID uint64, reason string) error {
	v.rebuildSessMu.Lock()
	defer v.rebuildSessMu.Unlock()
	if v.rebuildSess == nil {
		return fmt.Errorf("rebuild session: no active session")
	}
	if sessionID != 0 && v.rebuildSess.SessionID() != sessionID {
		return fmt.Errorf("rebuild session: session mismatch: have %d want %d", v.rebuildSess.SessionID(), sessionID)
	}
	if reason == "" {
		reason = "cancelled"
	}
	v.rebuildSess.Fail(reason)
	v.rebuildSess = nil
	return nil
}

// ActiveRebuildSession returns the current rebuild session snapshots.
func (v *BlockVol) ActiveRebuildSession() (RebuildSessionConfig, RebuildSessionProgress, bool) {
	v.rebuildSessMu.RLock()
	session := v.rebuildSess
	v.rebuildSessMu.RUnlock()
	if session == nil {
		return RebuildSessionConfig{}, RebuildSessionProgress{}, false
	}
	return session.Config(), session.Progress(), true
}

// ApplyRebuildSessionWALEntry routes one WAL entry into the active rebuild
// session after validating the session ID.
func (v *BlockVol) ApplyRebuildSessionWALEntry(sessionID uint64, entry *WALEntry) error {
	session, err := v.activeRebuildSession(sessionID)
	if err != nil {
		return err
	}
	return session.ApplyWALEntry(entry)
}

// ApplyRebuildSessionBaseBlock routes one base block into the active rebuild
// session after validating the session ID.
func (v *BlockVol) ApplyRebuildSessionBaseBlock(sessionID uint64, lba uint64, data []byte) (bool, error) {
	session, err := v.activeRebuildSession(sessionID)
	if err != nil {
		return false, err
	}
	return session.ApplyBaseBlock(lba, data)
}

// MarkRebuildSessionBaseComplete marks the active rebuild session's base lane as
// fully processed.
func (v *BlockVol) MarkRebuildSessionBaseComplete(sessionID uint64, totalBlocks uint64) error {
	session, err := v.activeRebuildSession(sessionID)
	if err != nil {
		return err
	}
	session.MarkBaseComplete(totalBlocks)
	return nil
}

// TryCompleteRebuildSession evaluates whether the active rebuild session has
// reached its dual completion gate.
func (v *BlockVol) TryCompleteRebuildSession(sessionID uint64) (uint64, bool, error) {
	session, err := v.activeRebuildSession(sessionID)
	if err != nil {
		return 0, false, err
	}
	achieved, completed := session.TryComplete()
	return achieved, completed, nil
}

func (v *BlockVol) ObserveRebuildSessionAppliedLSN(sessionID, appliedLSN uint64) error {
	sess, err := v.activeRebuildSession(sessionID)
	if err != nil {
		return err
	}
	ack := sess.ObserveAppliedLSN(appliedLSN)
	v.emitRebuildSessionAck(ack)
	return nil
}

func (v *BlockVol) activeRebuildSession(sessionID uint64) (*RebuildSession, error) {
	v.rebuildSessMu.RLock()
	session := v.rebuildSess
	v.rebuildSessMu.RUnlock()
	if session == nil {
		return nil, fmt.Errorf("rebuild session: no active session")
	}
	if sessionID == 0 || session.SessionID() != sessionID {
		return nil, fmt.Errorf("rebuild session: session mismatch: have %d want %d", session.SessionID(), sessionID)
	}
	return session, nil
}

func (v *BlockVol) emitRebuildSessionAck(ack SessionAckMsg) {
	if v == nil || v.onRebuildSessionAck == nil || ack.SessionID == 0 {
		return
	}
	v.onRebuildSessionAck(ack)
}

// applyRebuildWALEntry applies a WAL entry during rebuild without going
// through the normal write gate (epoch/role checks are session-level).
// The entry is appended to the local WAL and dirty map is updated.
func (v *BlockVol) applyRebuildWALEntry(entry *WALEntry) error {
	if v == nil {
		return fmt.Errorf("volume is nil")
	}
	v.ioMu.RLock()
	defer v.ioMu.RUnlock()

	walOff, err := v.wal.Append(entry)
	if err != nil {
		return err
	}
	// Update dirty map so ReadLBA sees the WAL data for each covered block.
	blocks := entry.Length / v.super.BlockSize
	if blocks == 0 {
		blocks = 1
	}
	for i := uint32(0); i < blocks; i++ {
		v.dirtyMap.Put(entry.LBA+uint64(i), walOff, entry.LSN, v.super.BlockSize)
	}
	return nil
}

// writeExtentDirect writes data directly to the extent file at the given LBA.
// Used by the base lane during rebuild when bitmap shows no WAL conflict.
// This bypasses the WAL — the data goes directly to the extent image.
//
// Takes ioMu.Lock (exclusive) to serialize against the flusher's extent
// writes. Without this, a race exists: WAL lane writes newer data → flusher
// flushes it to extent and deletes dirty map entry → base lane's older
// pwrite lands on the same extent offset → stale data becomes visible.
//
// Under the exclusive lock, we re-check the dirty map: if a WAL entry
// exists for this LBA, the base write is skipped (WAL data is newer and
// will be visible through the dirty map or after flusher flushes it).
//
// TODO(perf): Global ioMu.Lock is safe for MVP because the rebuilding
// replica is not serving frontend I/O. If live reads during rebuild are
// added later, switch to per-LBA striped locking to prevent flusher
// starvation and read latency during large rebuilds.
func (v *BlockVol) writeExtentDirect(lba uint64, data []byte) error {
	return v.writeExtentDirectForRebuild(lba, data, 0)
}

func (v *BlockVol) writeExtentDirectUnconditional(lba uint64, data []byte) error {
	return v.writeExtentDirectWithGuard(lba, data, false, 0)
}

func (v *BlockVol) writeExtentDirectForRebuild(lba uint64, data []byte, baseLSN uint64) error {
	return v.writeExtentDirectWithGuard(lba, data, true, baseLSN)
}

func (v *BlockVol) writeExtentDirectWithGuard(lba uint64, data []byte, guardDirtyMap bool, baseLSN uint64) error {
	if v == nil {
		return fmt.Errorf("volume is nil")
	}
	v.ioMu.RLock()
	defer v.ioMu.RUnlock()

	// Re-check dirty map: if WAL lane already wrote this LBA, skip the base
	// write. RLock is sufficient because pwrite at different file offsets
	// doesn't conflict, and the dirty map provides the necessary protection
	// against overwriting WAL-applied data.
	if guardDirtyMap {
		if _, lsn, _, ok := v.dirtyMap.Get(lba); ok && lsn > baseLSN {
			return nil // WAL data is newer, skip base write
		}
	}

	extentStart := v.super.WALOffset + v.super.WALSize
	offset := int64(extentStart) + int64(lba)*int64(v.super.BlockSize)
	_, err := v.fd.WriteAt(data, offset)
	return err
}
