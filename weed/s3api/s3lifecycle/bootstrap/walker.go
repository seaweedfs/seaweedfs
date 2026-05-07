// Package bootstrap implements the bucket-level lifecycle bootstrap walker.
//
// The walker iterates every entry under a bucket exactly once per bootstrap
// task, evaluates every active ActionKey against the entry, and dispatches
// inline-delete RPCs for currently-due actions. Not-yet-due entries are
// skipped entirely — the meta-log reader (Phase 3) picks them up via
// event-driven replay. Date-kind actions also skip here; their bucket-level
// SCAN_AT_DATE bootstrap fires once on the rule's date.
//
// The walker is callback-driven so callers can supply their own filer
// listing source (real filer_pb client or test fake) and their own delete
// dispatcher (worker-side LifecycleDelete client or test fake). Checkpoint
// state (`last_scanned_path`) is returned to the caller, who is responsible
// for persisting it under /etc/s3/lifecycle/<bucket>/_bootstrap.
package bootstrap

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// Entry is the minimal slice of a filer entry the walker needs. The caller
// converts *filer_pb.Entry into this shape so the walker stays free of
// filer_pb dependencies.
type Entry struct {
	Path           string // bucket-relative; no leading slash
	ModTime        time.Time
	Size           int64
	IsLatest       bool
	IsDeleteMarker bool
	IsMPUInit      bool
	NumVersions    int
	Tags           map[string]string

	// SuccessorModTime / NoncurrentIndex are populated for non-current
	// versions during versioned-bucket walks. Phase 5 fills these in;
	// Phase 2d non-versioned walks leave them at zero values, in which
	// case ComputeDueAt falls back to ModTime per AWS-conservative
	// semantics.
	SuccessorModTime time.Time
	NoncurrentIndex  int
}

// ListFunc enumerates entries under a bucket in lexicographic order.
// The callback is invoked once per entry; returning a non-nil error halts
// the walk and bubbles back through Walk. start is the resume point —
// entries with Path <= start are skipped (used after a kill-resume).
type ListFunc func(ctx context.Context, bucket, start string, cb func(*Entry) error) error

// Dispatcher executes one (action, entry) verdict. The walker calls this
// only for actions whose ComputeDueAt indicates the entry is currently due.
// Returning an error halts the walk; the caller decides whether to retry
// from the recorded last_scanned_path.
type Dispatcher interface {
	Delete(ctx context.Context, action *engine.CompiledAction, entry *Entry) error
}

// Checkpoint is the resume state the walker hands back. The caller persists
// it to /etc/s3/lifecycle/<bucket>/_bootstrap so a kill-resumed task picks
// up where this one stopped.
type Checkpoint struct {
	LastScannedPath string
	Completed       bool // true when the walk ran to completion of the listing
}

// WalkOptions tunes the walk. All fields have safe zero values.
type WalkOptions struct {
	// Resume from this path; entries with Path <= Resume are skipped.
	Resume string
	// Now is the wall-clock the walker uses for due-checks. Tests inject
	// a fixed time; callers default to time.Now().
	Now time.Time
}

// Walk iterates the bucket's entries via list, evaluates each entry against
// every ACTIVE ActionKey for the bucket in snap, and dispatches Delete
// for currently-due actions. Returns a Checkpoint the caller persists.
//
// Per-entry semantics:
//   - prefix-mismatched / filter-rejected actions: skipped (no dispatch).
//   - not-yet-due actions: skipped (the reader will pick them up via the
//     meta log when the cutoff sweeps past the entry).
//   - currently-due actions: Dispatcher.Delete called. Errors halt the walk.
//   - SCAN_AT_DATE actions: skipped (their date-triggered bootstrap is a
//     separate task).
//
// The walker uses MatchPath to enumerate candidate ActionKeys per entry
// (already filtered by prefix and active), then EvaluateAction per kind to
// confirm the entry is actually due.
func Walk(ctx context.Context, snap *engine.Snapshot, bucket string, list ListFunc, dispatch Dispatcher, opts WalkOptions) (Checkpoint, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cp := Checkpoint{LastScannedPath: opts.Resume}

	err := list(ctx, bucket, opts.Resume, func(entry *Entry) error {
		if entry == nil || entry.Path == "" {
			return nil
		}
		if opts.Resume != "" && entry.Path <= opts.Resume {
			return nil
		}
		if err := walkEntry(ctx, snap, bucket, entry, dispatch, now); err != nil {
			return err
		}
		cp.LastScannedPath = entry.Path
		return nil
	})
	if err != nil {
		return cp, err
	}
	cp.Completed = true
	return cp, nil
}

// walkEntry runs the per-entry portion of the walk: enumerate matching
// ActionKeys and dispatch the ones currently due.
func walkEntry(ctx context.Context, snap *engine.Snapshot, bucket string, entry *Entry, dispatch Dispatcher, now time.Time) error {
	// MatchPath returns prefix-and-filter-matched ACTIVE ActionKeys; we
	// pass nil for the Event since the bootstrap walker has full live
	// state per entry already.
	keys := snap.MatchPath(bucket, entry.Path, nil)
	for _, key := range keys {
		action := snap.Action(key)
		if action == nil {
			continue
		}
		// SCAN_AT_DATE actions don't dispatch from the walker; their
		// date-triggered bootstrap is scheduled separately.
		if action.Mode == engine.ModeScanAtDate {
			continue
		}
		// Bootstrap evaluates with EvaluateAction on the live entry's
		// ObjectInfo shape. We construct the ObjectInfo from the walker
		// Entry so callers don't have to.
		info := &s3lifecycle.ObjectInfo{
			Key:              entry.Path,
			ModTime:          entry.ModTime,
			Size:             entry.Size,
			IsLatest:         entry.IsLatest,
			IsDeleteMarker:   entry.IsDeleteMarker,
			IsMPUInit:        entry.IsMPUInit,
			NumVersions:      entry.NumVersions,
			SuccessorModTime: entry.SuccessorModTime,
			NoncurrentIndex:  entry.NoncurrentIndex,
			Tags:             entry.Tags,
		}
		res := s3lifecycle.EvaluateAction(action.Rule, key.ActionKind, info, now)
		if res.Action == s3lifecycle.ActionNone {
			continue
		}
		if err := dispatch.Delete(ctx, action, entry); err != nil {
			glog.Warningf("lifecycle bootstrap: dispatch %s/%s kind=%s: %v",
				bucket, entry.Path, key.ActionKind, err)
			return err
		}
	}
	return nil
}

// EntryCallback is a convenience type for simple in-memory listings. Tests
// typically build a ListFunc by closing over a sorted slice.
func EntryCallback(entries []*Entry) ListFunc {
	return func(ctx context.Context, bucket, start string, cb func(*Entry) error) error {
		for _, e := range entries {
			if start != "" && e.Path <= start {
				continue
			}
			if err := cb(e); err != nil {
				return err
			}
		}
		return nil
	}
}

// HasPrefix is a small helper kept here so the walker stays self-contained.
// It mirrors strings.HasPrefix but feels natural at the package surface.
func HasPrefix(path, prefix string) bool { return strings.HasPrefix(path, prefix) }
