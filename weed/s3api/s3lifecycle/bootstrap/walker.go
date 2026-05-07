// Package bootstrap is the bucket-level lifecycle walker. The walker
// iterates every entry under a bucket, evaluates every active ActionKey
// against it, and dispatches inline-delete RPCs for currently-due actions;
// not-yet-due entries are left for the meta-log reader to pick up later.
//
// Callback-driven so the listing source and the LifecycleDelete dispatcher
// can be supplied separately (real client or test fake).
package bootstrap

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// Entry is the minimal slice of a filer entry the walker needs.
// IsDirectory marks SeaweedFS directory entries; the walker never
// dispatches against directories. SuccessorModTime and NoncurrentIndex are
// populated only for versioned-bucket walks (Phase 5); non-versioned walks
// leave them at zero / nil and the count-based retention path bails out
// conservatively.
type Entry struct {
	Path           string
	ModTime        time.Time
	Size           int64
	IsDirectory    bool
	IsLatest       bool
	IsDeleteMarker bool
	IsMPUInit      bool
	NumVersions    int
	Tags           map[string]string

	SuccessorModTime time.Time
	NoncurrentIndex  *int
}

// ListFunc must skip entries with Path <= start so kill-resume picks up
// where the previous run stopped.
type ListFunc func(ctx context.Context, bucket, start string, cb func(*Entry) error) error

// Dispatcher executes one (action, entry) verdict. An error halts the walk;
// the caller decides whether to retry from the recorded last_scanned_path.
type Dispatcher interface {
	Delete(ctx context.Context, action *engine.CompiledAction, entry *Entry) error
}

// Checkpoint is the resume state. Caller persists it under
// /etc/s3/lifecycle/<bucket>/_bootstrap.
type Checkpoint struct {
	LastScannedPath string
	Completed       bool
}

type WalkOptions struct {
	Resume string
	Now    time.Time
}

// Walk iterates entries via list, evaluates each active ActionKey via
// MatchPath + EvaluateAction, and calls Dispatcher.Delete for currently-due
// actions. SCAN_AT_DATE actions are skipped (their bootstrap is scheduled
// separately).
func Walk(ctx context.Context, snap *engine.Snapshot, bucket string, list ListFunc, dispatch Dispatcher, opts WalkOptions) (Checkpoint, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cp := Checkpoint{LastScannedPath: opts.Resume}

	// Single ObjectInfo reused across the walk; EvaluateAction reads the
	// fields synchronously and doesn't retain references, so per-entry
	// field assignments are safe and avoid one heap allocation per entry
	// for buckets with millions of objects.
	var info s3lifecycle.ObjectInfo

	err := list(ctx, bucket, opts.Resume, func(entry *Entry) error {
		if entry == nil || entry.Path == "" {
			return nil
		}
		// Lifecycle rules only apply to objects; SeaweedFS directory
		// entries can co-exist in the listing and must not be deleted.
		if entry.IsDirectory {
			cp.LastScannedPath = entry.Path
			return nil
		}
		if err := walkEntry(ctx, snap, bucket, entry, dispatch, now, &info); err != nil {
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

func walkEntry(ctx context.Context, snap *engine.Snapshot, bucket string, entry *Entry, dispatch Dispatcher, now time.Time, info *s3lifecycle.ObjectInfo) error {
	keys := snap.MatchPath(bucket, entry.Path, nil)
	if len(keys) == 0 {
		return nil
	}
	// Reuse the caller-provided ObjectInfo (one allocation per Walk, not
	// per entry); EvaluateAction reads synchronously without retaining.
	*info = s3lifecycle.ObjectInfo{
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
	for _, key := range keys {
		action := snap.Action(key)
		if action == nil {
			continue
		}
		// SCAN_AT_DATE has its own date-triggered bootstrap; DISABLED can be
		// flipped at runtime by the operator independent of XML rule status,
		// so explicitly skip it here even though EvaluateAction also gates
		// on the rule's Status.
		if action.Mode == engine.ModeScanAtDate || action.Mode == engine.ModeDisabled {
			continue
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

// EntryCallback wraps an in-memory slice as a ListFunc; useful for tests.
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

func HasPrefix(path, prefix string) bool { return strings.HasPrefix(path, prefix) }
