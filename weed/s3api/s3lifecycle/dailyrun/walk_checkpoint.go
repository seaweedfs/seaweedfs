package dailyrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// WalkCheckpoint is the saved progress of an unfinished WalkBuckets
// call. ViewHash (engine.WalkContentHash) and Shards identify the walk;
// a walk with another identity starts over.
type WalkCheckpoint struct {
	ViewHash [32]byte
	Shards   []int
	// Done lists finished buckets; Resume maps a partly walked bucket
	// to its bootstrap.Checkpoint.LastScannedPath.
	Done   []string
	Resume map[string]string
	// Set by LoadWalk; SaveWalk stamps the current time.
	SavedAt time.Time
}

// WalkCheckpointPersister keeps one WalkCheckpoint per shard set.
type WalkCheckpointPersister interface {
	LoadWalk(ctx context.Context, shards []int) (WalkCheckpoint, bool, error)
	SaveWalk(ctx context.Context, cp WalkCheckpoint) error
}

type walkCheckpointFile struct {
	Version  int               `json:"version"`
	Shards   []int             `json:"shards"`
	ViewHash []byte            `json:"view_hash,omitempty"`
	Done     []string          `json:"done,omitempty"`
	Resume   map[string]string `json:"resume,omitempty"`
	SavedNs  int64             `json:"saved_ns,omitempty"`
}

func walkCheckpointFileName(shards []int) string {
	contiguous := len(shards) > 1
	for i := 1; i < len(shards); i++ {
		contiguous = contiguous && shards[i] == shards[i-1]+1
	}
	if contiguous {
		return fmt.Sprintf("walk-%02d-%02d.json", shards[0], shards[len(shards)-1])
	}
	parts := make([]string, len(shards))
	for i, sh := range shards {
		parts[i] = fmt.Sprintf("%02d", sh)
	}
	return "walk-" + strings.Join(parts, "_") + ".json"
}

func (p *FilerCursorPersister) LoadWalk(ctx context.Context, shards []int) (WalkCheckpoint, bool, error) {
	if p.Store == nil {
		return WalkCheckpoint{}, false, errors.New("FilerCursorPersister: nil Store")
	}
	content, err := p.Store.Read(ctx, CursorDir, walkCheckpointFileName(shards))
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return WalkCheckpoint{}, false, nil
		}
		return WalkCheckpoint{}, false, fmt.Errorf("walk checkpoint read: %w", err)
	}
	var f walkCheckpointFile
	if err := json.Unmarshal(content, &f); err != nil {
		return WalkCheckpoint{}, false, fmt.Errorf("walk checkpoint decode: %w", err)
	}
	if f.Version != cursorFileVersion {
		return WalkCheckpoint{}, false, fmt.Errorf("walk checkpoint version: got %d, want %d", f.Version, cursorFileVersion)
	}
	if len(f.ViewHash) != 0 && len(f.ViewHash) != 32 {
		return WalkCheckpoint{}, false, fmt.Errorf("walk checkpoint view_hash: got %d bytes, want 32", len(f.ViewHash))
	}
	cp := WalkCheckpoint{Shards: f.Shards, Done: f.Done, Resume: f.Resume, SavedAt: time.Unix(0, f.SavedNs)}
	copy(cp.ViewHash[:], f.ViewHash)
	return cp, true, nil
}

func (p *FilerCursorPersister) SaveWalk(ctx context.Context, cp WalkCheckpoint) error {
	if p.Store == nil {
		return errors.New("FilerCursorPersister: nil Store")
	}
	f := walkCheckpointFile{Version: cursorFileVersion, Shards: cp.Shards, Done: cp.Done, Resume: cp.Resume, SavedNs: time.Now().UnixNano()}
	if cp.ViewHash != ([32]byte{}) {
		f.ViewHash = cp.ViewHash[:]
	}
	content, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("walk checkpoint encode: %w", err)
	}
	if err := p.Store.Save(ctx, CursorDir, walkCheckpointFileName(cp.Shards), content); err != nil {
		return fmt.Errorf("walk checkpoint save: %w", err)
	}
	return nil
}

// walkCheckpointInterval spaces the saves of a running walk, in ns.
var walkCheckpointInterval atomic.Int64

// A walk that keeps failing saves on every attempt; a record this old
// belongs to a shard set that stopped walking, and resuming it would
// skip buckets walked long ago.
const walkCheckpointMaxAge = 7 * 24 * time.Hour

func init() { walkCheckpointInterval.Store(int64(30 * time.Second)) }

// walkProgress tracks one WalkBuckets call against its checkpoint.
type walkProgress struct {
	store      WalkCheckpointPersister
	cp         WalkCheckpoint
	done       map[string]bool
	onDisk     bool // a saved record a finished walk must clear
	resumed    bool
	progressed bool
	savedAt    time.Time
}

func loadWalkProgress(ctx context.Context, store WalkCheckpointPersister, view *engine.Snapshot, shards []int, buckets []string) *walkProgress {
	sorted := slices.Clone(shards)
	slices.Sort(sorted)
	w := &walkProgress{
		store:   store,
		cp:      WalkCheckpoint{ViewHash: engine.WalkContentHash(view), Shards: sorted, Resume: map[string]string{}},
		done:    map[string]bool{},
		savedAt: time.Now(),
	}
	if store == nil {
		return w
	}
	saved, found, err := store.LoadWalk(ctx, sorted)
	if err != nil {
		glog.Warningf("walker: shards %v: %v; walking every bucket", sorted, err)
		w.onDisk = true
		return w
	}
	if !found {
		return w
	}
	w.onDisk = len(saved.Done) > 0 || len(saved.Resume) > 0
	if saved.ViewHash != w.cp.ViewHash || !slices.Equal(saved.Shards, sorted) || time.Since(saved.SavedAt) > walkCheckpointMaxAge {
		return w
	}
	listed := make(map[string]bool, len(buckets))
	for _, b := range buckets {
		listed[b] = true
	}
	for _, b := range saved.Done {
		if listed[b] {
			w.done[b] = true
		}
	}
	for b, pos := range saved.Resume {
		if listed[b] && !w.done[b] {
			w.cp.Resume[b] = pos
		}
	}
	w.resumed = len(w.done) > 0 || len(w.cp.Resume) > 0
	if w.resumed {
		glog.V(0).Infof("walker: shards %v resume with %d bucket(s) done and %d partly walked", sorted, len(w.done), len(w.cp.Resume))
	}
	return w
}

// track records each finished entry's position as the bucket's resume
// point, saving it every walkCheckpointInterval.
func (w *walkProgress) track(bucket string, list bootstrap.ListFunc) bootstrap.ListFunc {
	if w.store == nil {
		return list
	}
	return func(ctx context.Context, b, start string, cb func(*bootstrap.Entry) error) error {
		return list(ctx, b, start, func(e *bootstrap.Entry) error {
			if err := cb(e); err != nil {
				return err
			}
			if e != nil && e.Resume != "" {
				w.cp.Resume[bucket] = e.Resume
				w.progressed = true
				w.maybeSave(ctx)
			}
			return nil
		})
	}
}

func (w *walkProgress) bucketDone(ctx context.Context, bucket string) {
	w.done[bucket] = true
	delete(w.cp.Resume, bucket)
	w.progressed = true
	w.maybeSave(ctx)
}

func (w *walkProgress) maybeSave(ctx context.Context) {
	if w.store == nil || time.Since(w.savedAt) < time.Duration(walkCheckpointInterval.Load()) {
		return
	}
	if err := w.save(ctx); err != nil {
		glog.Warningf("walker: shards %v: %v", w.cp.Shards, err)
	}
}

func (w *walkProgress) save(ctx context.Context) error {
	cp := w.cp
	cp.Done = make([]string, 0, len(w.done))
	for b := range w.done {
		cp.Done = append(cp.Done, b)
	}
	slices.Sort(cp.Done)
	if err := w.store.SaveWalk(ctx, cp); err != nil {
		return err
	}
	w.savedAt = time.Now()
	w.onDisk = len(cp.Done) > 0 || len(cp.Resume) > 0
	return nil
}

// finish saves where the walk stopped, or clears the checkpoint when it
// completed. A resumed walk that failed again without progress is
// cleared too, so one stuck bucket cannot keep the finished ones from
// ever being walked again. Any other failed walk without progress has
// nothing new to record and leaves the file, read or not, alone.
func (w *walkProgress) finish(ctx context.Context, walkErr error) error {
	if w.store == nil {
		return nil
	}
	reset := walkErr == nil
	if walkErr != nil && !w.progressed {
		if ctx.Err() != nil || !w.resumed {
			return nil
		}
		glog.Warningf("walker: shards %v made no progress on a resumed walk; the next walk starts over", w.cp.Shards)
		reset = true
	}
	if reset {
		clear(w.done)
		clear(w.cp.Resume)
	}
	if !w.onDisk && len(w.done) == 0 && len(w.cp.Resume) == 0 {
		return nil
	}
	saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.save(saveCtx); err != nil {
		if walkErr == nil {
			// Left in place, the record would skip buckets next walk.
			return fmt.Errorf("walker: shards %v: %w", w.cp.Shards, err)
		}
		glog.Warningf("walker: shards %v: %v", w.cp.Shards, err)
	}
	return nil
}
