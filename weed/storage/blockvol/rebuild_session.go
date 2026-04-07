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
	SessionID  uint64
	Epoch      uint64
	BaseLSN    uint64 // snapshot point-in-time LSN
	TargetLSN  uint64 // WAL must reach this before completion
	SnapshotID uint32 // snapshot to use as base (0 = use current extent)
}

// RebuildSession manages one replica-side rebuild session with two concurrent
// data lanes:
//
//   - Base lane: trusted snapshot/extent blocks applied with bitmap protection
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
	if config.Epoch == 0 {
		return nil, fmt.Errorf("rebuild session: epoch is required")
	}

	info := vol.Info()
	totalLBAs := info.VolumeSize / uint64(info.BlockSize)
	bitmap := NewRebuildBitmap(totalLBAs, info.BlockSize)

	return &RebuildSession{
		config: config,
		phase:  RebuildPhaseAccepted,
		bitmap: bitmap,
		vol:    vol,
	}, nil
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

// ApplyWALEntry applies one WAL entry through the WAL lane. The entry is
// applied to the replica's local WAL, and the bitmap bit is set for each
// LBA covered by the entry. This ensures base lane data for the same LBA
// will be skipped (WAL always wins).
//
// The bitmap bit is set AFTER successful WAL append (applied), not on
// receive. This is the key correctness invariant.
func (s *RebuildSession) ApplyWALEntry(entry *WALEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != RebuildPhaseRunning && s.phase != RebuildPhaseBaseComplete {
		return fmt.Errorf("rebuild session: WAL apply not allowed in phase %s", s.phase)
	}
	if entry.Epoch != s.config.Epoch {
		return fmt.Errorf("rebuild session: epoch mismatch: entry=%d session=%d", entry.Epoch, s.config.Epoch)
	}

	// Apply to local WAL via the volume's WAL writer.
	if err := s.vol.applyRebuildWALEntry(entry); err != nil {
		return fmt.Errorf("rebuild session: WAL apply LSN=%d: %w", entry.LSN, err)
	}

	// AFTER successful apply: mark bitmap for each LBA covered by this entry.
	if entry.Type == EntryTypeWrite && entry.Length > 0 {
		blockSize := uint64(s.config.blockSize())
		if blockSize == 0 {
			blockSize = uint64(s.vol.Info().BlockSize)
		}
		startLBA := entry.LBA
		blocks := uint64(entry.Length) / blockSize
		if blocks == 0 {
			blocks = 1
		}
		for i := uint64(0); i < blocks; i++ {
			s.bitmap.MarkApplied(startLBA + i)
		}
	}

	if entry.LSN > s.walAppliedLSN {
		s.walAppliedLSN = entry.LSN
	}
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
	defer s.mu.Unlock()
	if s.phase != RebuildPhaseRunning {
		return false, fmt.Errorf("rebuild session: base apply not allowed in phase %s", s.phase)
	}

	// Bitmap conflict check: WAL-applied LBA wins.
	if !s.bitmap.ShouldApplyBase(lba) {
		s.baseBlocksSkipped++
		return false, nil
	}

	// Apply base block directly to the extent (not through WAL).
	if err := s.vol.writeExtentDirect(lba, data); err != nil {
		return false, fmt.Errorf("rebuild session: base apply LBA=%d: %w", lba, err)
	}

	s.baseBlocksApplied++
	return true, nil
}

// MarkBaseComplete marks the base lane as fully transferred.
// The session transitions to BaseComplete phase if currently Running.
func (s *RebuildSession) MarkBaseComplete(totalBlocks uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.baseBlocksTotal = totalBlocks
	s.baseComplete = true
	if s.phase == RebuildPhaseRunning {
		s.phase = RebuildPhaseBaseComplete
	}
}

// TryComplete checks if both completion conditions are met:
//  1. base_complete = true
//  2. wal_applied_lsn >= target_lsn
//
// If both are true, transitions to Completed phase and returns the achieved LSN.
// If not ready, returns (0, false).
func (s *RebuildSession) TryComplete() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.baseComplete {
		return 0, false
	}
	if s.walAppliedLSN < s.config.TargetLSN {
		return 0, false
	}
	if s.phase == RebuildPhaseCompleted || s.phase == RebuildPhaseFailed {
		return 0, false
	}
	s.phase = RebuildPhaseCompleted
	return s.walAppliedLSN, true
}

// Fail marks the session as failed with a reason.
func (s *RebuildSession) Fail(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase = RebuildPhaseFailed
	s.failReason = reason
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

// StartRebuildSession installs and starts one active rebuild session on the
// replica. A new session supersedes any previous active rebuild session.
func (v *BlockVol) StartRebuildSession(config RebuildSessionConfig) error {
	if config.SessionID == 0 {
		return fmt.Errorf("rebuild session: session ID is required")
	}
	session, err := NewRebuildSession(v, config)
	if err != nil {
		return err
	}
	if err := session.Start(); err != nil {
		return err
	}

	v.rebuildSessMu.Lock()
	defer v.rebuildSessMu.Unlock()
	if v.rebuildSess != nil {
		v.rebuildSess.Fail("superseded")
	}
	v.rebuildSess = session
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
	// Update dirty map so ReadLBA sees the WAL data.
	v.dirtyMap.Put(entry.LBA, walOff, entry.LSN, entry.Length)
	return nil
}

// writeExtentDirect writes data directly to the extent file at the given LBA.
// Used by the base lane during rebuild when bitmap shows no WAL conflict.
// This bypasses the WAL — the data goes directly to the extent image.
func (v *BlockVol) writeExtentDirect(lba uint64, data []byte) error {
	if v == nil {
		return fmt.Errorf("volume is nil")
	}
	v.ioMu.RLock()
	defer v.ioMu.RUnlock()

	extentStart := v.super.WALOffset + v.super.WALSize
	offset := int64(extentStart) + int64(lba)*int64(v.super.BlockSize)
	_, err := v.fd.WriteAt(data, offset)
	return err
}
