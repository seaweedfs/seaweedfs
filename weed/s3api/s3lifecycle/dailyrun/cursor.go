// Package dailyrun implements the daily-replay s3 lifecycle worker
// described in weed/s3api/s3lifecycle/DESIGN.md. One pass per day per
// shard reads the meta-log forward from the persisted cursor, drains
// every event whose due_time is past now, and exits — replacing the
// streaming + heap pipeline with a bounded, idempotent scan.
//
// Phase 2 (this file's first version): replay-only. Buckets whose
// compiled rules include walker-bound action kinds (ExpirationDate,
// ExpiredDeleteMarker, NewerNoncurrent) or any scan_only promotion are
// refused with a typed error so flipping the algorithm flag is a loud
// failure rather than silent data loss. Phase 4 wires the walker and
// the recovery branch.
package dailyrun

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
)

// CursorDir is the filer directory holding per-shard daily-replay
// cursor files. Kept distinct from dispatcher.CursorDir so a deployment
// running both algorithms during cutover doesn't have one trample the
// other's persisted state.
const CursorDir = "/etc/s3/lifecycle/daily-cursors"

// cursorFileVersion bumps when the on-disk shape changes. Phase 2
// writes version 1; Phase 4 will continue writing version 1 since the
// schema (TsNs + RuleSetHash + PromotedHash) is already final.
const cursorFileVersion = 1

// Cursor captures everything daily_run needs to decide whether to
// recover (Phase 4) or continue steady-state. TsNs is the latest
// meta-log event whose Matches all dispatched successfully (or as
// NOOP_RESOLVED); RuleSetHash is the content hash of the replay-eligible
// rules from the run that wrote this cursor; PromotedHash is the hash
// of replay-eligible rules currently in scan_only.
//
// In Phase 2 PromotedHash is always the empty hash because the walker
// path isn't wired yet — any rule that would land in walk causes the
// run to refuse. Phase 4 starts writing a real value.
type Cursor struct {
	TsNs         int64
	RuleSetHash  [32]byte
	PromotedHash [32]byte
}

// cursorFile is the on-disk JSON shape. Bytes are base64-encoded by
// encoding/json automatically.
type cursorFile struct {
	Version      int    `json:"version"`
	ShardID      int    `json:"shard_id"`
	TsNs         int64  `json:"ts_ns"`
	RuleSetHash  []byte `json:"rule_set_hash"`
	PromotedHash []byte `json:"promoted_hash"`
}

// CursorPersister loads and saves daily-replay cursors. The Phase 2
// production implementation is FilerCursorPersister; tests inject a
// fake.
type CursorPersister interface {
	Load(ctx context.Context, shardID int) (Cursor, bool, error) // (cursor, found, err)
	Save(ctx context.Context, shardID int, c Cursor) error
}

// FilerCursorPersister writes cursors to CursorDir as one JSON file per
// shard. Reuses dispatcher.FilerStore for the actual filer I/O so both
// algorithms share the same minimal storage abstraction.
type FilerCursorPersister struct {
	Store dispatcher.FilerStore
}

func cursorFileName(shardID int) string {
	return fmt.Sprintf("shard-%02d.json", shardID)
}

// Load returns (zero-Cursor, false, nil) only when the cursor file
// does not exist yet (cold start). Every other failure mode — empty
// file, malformed JSON, wrong version, wrong shard, hash slices not
// exactly 32 bytes — returns an error so the daily run halts and is
// fixed by an operator rather than silently re-scanning from time zero.
//
// Strict shape validation is load-bearing because the cursor is
// load → mutate → save in a single run: a partial-truncate that
// silently zero-padded the rule_set_hash would be persisted back as a
// real-looking hash, masking the corruption forever.
func (p *FilerCursorPersister) Load(ctx context.Context, shardID int) (Cursor, bool, error) {
	if p.Store == nil {
		return Cursor{}, false, errors.New("FilerCursorPersister: nil Store")
	}
	content, err := p.Store.Read(ctx, CursorDir, cursorFileName(shardID))
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return Cursor{}, false, nil
		}
		return Cursor{}, false, fmt.Errorf("cursor read shard=%d: %w", shardID, err)
	}
	if len(content) == 0 {
		return Cursor{}, false, fmt.Errorf("cursor shard=%d: file exists but is empty (partial write or external truncation)", shardID)
	}
	var cf cursorFile
	if err := json.Unmarshal(content, &cf); err != nil {
		return Cursor{}, false, fmt.Errorf("cursor decode shard=%d: %w", shardID, err)
	}
	if cf.Version != cursorFileVersion {
		return Cursor{}, false, fmt.Errorf("cursor version shard=%d: got %d, want %d", shardID, cf.Version, cursorFileVersion)
	}
	if cf.ShardID != shardID {
		return Cursor{}, false, fmt.Errorf("cursor shard mismatch: file declares shard=%d, requested shard=%d", cf.ShardID, shardID)
	}
	if len(cf.RuleSetHash) != 32 {
		return Cursor{}, false, fmt.Errorf("cursor rule_set_hash shard=%d: got %d bytes, want 32", shardID, len(cf.RuleSetHash))
	}
	if len(cf.PromotedHash) != 32 {
		return Cursor{}, false, fmt.Errorf("cursor promoted_hash shard=%d: got %d bytes, want 32", shardID, len(cf.PromotedHash))
	}
	c := Cursor{TsNs: cf.TsNs}
	copy(c.RuleSetHash[:], cf.RuleSetHash)
	copy(c.PromotedHash[:], cf.PromotedHash)
	return c, true, nil
}

func (p *FilerCursorPersister) Save(ctx context.Context, shardID int, c Cursor) error {
	if p.Store == nil {
		return errors.New("FilerCursorPersister: nil Store")
	}
	cf := cursorFile{
		Version:      cursorFileVersion,
		ShardID:      shardID,
		TsNs:         c.TsNs,
		RuleSetHash:  c.RuleSetHash[:],
		PromotedHash: c.PromotedHash[:],
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cf); err != nil {
		return fmt.Errorf("cursor encode shard=%d: %w", shardID, err)
	}
	if err := p.Store.Save(ctx, CursorDir, cursorFileName(shardID), buf.Bytes()); err != nil {
		return fmt.Errorf("cursor save shard=%d: %w", shardID, err)
	}
	return nil
}
