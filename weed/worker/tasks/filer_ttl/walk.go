package filer_ttl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	walkPageSize       = 1024
	checkpointDir      = filer.DirectoryEtcSeaweedFS + "/filer_ttl"
	checkpointMaxAge   = 7 * 24 * time.Hour
	checkpointSaveWait = 5 * time.Second
)

var checkpointInterval = 30 * time.Second

type walkStats struct {
	directories int64
	entries     int64
}

// walkCheckpoint is where an unfinished pass over Prefixes stopped: every
// prefix before Prefix is done, and inside Prefix every entry up to and
// including Cursor, with its subtree, is done.
type walkCheckpoint struct {
	Prefixes  []string `json:"prefixes"`
	Prefix    string   `json:"prefix"`
	Cursor    string   `json:"cursor,omitempty"`
	StartedNs int64    `json:"started_ns"`
}

// ttlLocationPrefixes returns the TTL path rules that no other TTL rule
// encloses, so each subtree is walked once.
func ttlLocationPrefixes(fc *filer.FilerConf) []string {
	var prefixes []string
	for _, rule := range fc.ToProto().Locations {
		ttl, err := needle.ReadTTL(rule.GetTtl())
		if err != nil || ttl.ToSeconds() == 0 {
			continue
		}
		prefixes = append(prefixes, rule.LocationPrefix)
	}
	sort.Strings(prefixes)
	var outer []string
	for _, prefix := range prefixes {
		if n := len(outer); n > 0 && strings.HasPrefix(prefix, outer[n-1]) {
			continue
		}
		outer = append(outer, prefix)
	}
	return outer
}

// checkpointName is keyed by the store signature, so a record that
// filer.sync copies into another cluster's store is never read there.
func checkpointName(signature int32) string {
	return fmt.Sprintf("walk-%d.json", signature)
}

type walker struct {
	client  filer_pb.SeaweedFilerClient
	name    string
	stats   *walkStats
	cp      walkCheckpoint
	onDisk  bool
	frozen  bool
	stored  bool
	savedAt time.Time
}

// walkLocationPrefixes lists every entry under the filer.conf location
// prefixes, continuing a pass an earlier run left unfinished. Listing is
// what removes expired rows: the filer drops each expired entry it meets
// and deletes the directories that leaves empty.
func walkLocationPrefixes(ctx context.Context, client filer_pb.SeaweedFilerClient, prefixes []string, stats *walkStats) (resumed string, err error) {
	prefixes = slices.Clone(prefixes)
	sort.Strings(prefixes)
	conf, err := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
	if err != nil {
		return "", fmt.Errorf("filer configuration: %w", err)
	}
	w := &walker{client: client, name: checkpointName(conf.Signature), stats: stats, savedAt: time.Now()}
	saved := w.load(ctx, prefixes)
	w.cp = walkCheckpoint{Prefixes: prefixes, StartedNs: time.Now().UnixNano()}
	if saved != nil {
		w.cp.StartedNs = saved.StartedNs
		resumed = saved.Prefix
		if saved.Cursor != "" {
			resumed = saved.Cursor
		}
	}

	listCtx := filer_pb.WithChunksOmitted(ctx)
	var failed []string
	for _, prefix := range prefixes {
		cursor := ""
		if saved != nil {
			if prefix < saved.Prefix {
				continue
			}
			if prefix == saved.Prefix {
				cursor = saved.Cursor
			}
		}
		if err := ctx.Err(); err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", prefix, err))
			w.stop(prefix, cursor)
			break
		}
		if !w.frozen {
			w.cp.Prefix, w.cp.Cursor = prefix, cursor
		}
		if err := w.walkLocationPrefix(listCtx, prefix, cursor); err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", prefix, err))
			w.stop(w.cp.Prefix, w.cp.Cursor)
		}
	}
	if len(failed) > 0 {
		if !w.stored {
			w.persist()
		}
		return resumed, errors.New(strings.Join(failed, "; "))
	}
	return resumed, w.clear(ctx)
}

func (w *walker) load(ctx context.Context, prefixes []string) *walkCheckpoint {
	content, err := filer.ReadInsideFiler(ctx, w.client, checkpointDir, w.name)
	if err != nil {
		if !errors.Is(err, filer_pb.ErrNotFound) {
			glog.Warningf("filer_ttl: read checkpoint: %v; walking every prefix", err)
			w.onDisk = true
		}
		return nil
	}
	w.onDisk = true
	var cp walkCheckpoint
	if err := json.Unmarshal(content, &cp); err != nil {
		glog.Warningf("filer_ttl: decode checkpoint: %v; walking every prefix", err)
		return nil
	}
	if !slices.Equal(cp.Prefixes, prefixes) || !slices.Contains(prefixes, cp.Prefix) || time.Since(time.Unix(0, cp.StartedNs)) > checkpointMaxAge {
		return nil
	}
	return &cp
}

// stop saves the checkpoint at the first place this run failed; later
// prefixes still get walked but are not recorded as done.
func (w *walker) stop(prefix, cursor string) {
	if w.frozen {
		return
	}
	w.cp.Prefix, w.cp.Cursor = prefix, cursor
	w.frozen = true
	w.persist()
}

func (w *walker) advance(ctx context.Context, done util.FullPath) {
	if w.frozen {
		return
	}
	w.cp.Cursor = string(done)
	if time.Since(w.savedAt) < checkpointInterval {
		return
	}
	w.savedAt = time.Now()
	if err := w.save(ctx); err != nil {
		glog.Warningf("filer_ttl: save checkpoint: %v", err)
	}
}

func (w *walker) persist() {
	ctx, cancel := context.WithTimeout(context.Background(), checkpointSaveWait)
	defer cancel()
	err := w.save(ctx)
	w.stored = err == nil
	if err != nil {
		glog.Warningf("filer_ttl: save checkpoint: %v", err)
	}
}

func (w *walker) save(ctx context.Context) error {
	content, err := json.Marshal(w.cp)
	if err != nil {
		return err
	}
	w.onDisk = true
	// An explicit TTL stops a filer.conf TTL rule over /etc/ from expiring
	// the record before load would stop using it.
	now := time.Now().Unix()
	return filer_pb.CreateEntry(ctx, w.client, &filer_pb.CreateEntryRequest{
		Directory: checkpointDir,
		Entry: &filer_pb.Entry{
			Name:    w.name,
			Content: content,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    now,
				Crtime:   now,
				FileMode: 0644,
				FileSize: uint64(len(content)),
				Md5:      util.Md5(content),
				TtlSec:   int32(checkpointMaxAge / time.Second),
			},
		},
	})
}

func (w *walker) clear(ctx context.Context) error {
	if !w.onDisk {
		return nil
	}
	if err := filer_pb.DoRemove(ctx, w.client, checkpointDir, w.name, false, false, false, false, nil); err != nil {
		return fmt.Errorf("clear checkpoint: %w", err)
	}
	return nil
}

func (w *walker) walkLocationPrefix(ctx context.Context, locationPrefix, cursor string) error {
	root, namePrefix := util.FullPath(locationPrefix), ""
	if !strings.HasSuffix(locationPrefix, "/") {
		dir, name := root.DirAndName()
		root, namePrefix = util.FullPath(dir), name
	}
	return w.walkDirectory(ctx, root, namePrefix, cursorPath(root, cursor))
}

// cursorPath splits a checkpoint cursor into its path below root.
func cursorPath(root util.FullPath, cursor string) []string {
	base := strings.TrimSuffix(string(root), "/") + "/"
	if cursor == "" || !strings.HasPrefix(cursor, base) || cursor == base {
		return nil
	}
	return strings.Split(strings.TrimPrefix(cursor, base), "/")
}

// walkDirectory lists dir depth first in name order. A non-empty resume
// path names the last finished entry: the walk descends into its
// unfinished ancestors and continues each one after them.
func (w *walker) walkDirectory(ctx context.Context, dir util.FullPath, namePrefix string, resume []string) error {
	w.stats.directories++
	startFrom := ""
	if len(resume) > 0 {
		startFrom = resume[0]
		if len(resume) > 1 {
			if err := w.walkDirectory(ctx, dir.Child(startFrom), "", resume[1:]); err != nil {
				return err
			}
			w.advance(ctx, dir.Child(startFrom))
		}
	}
	for {
		var subDirs []util.FullPath
		listed := 0
		err := filer_pb.SeaweedList(ctx, w.client, string(dir), namePrefix, func(entry *filer_pb.Entry, isLast bool) error {
			listed++
			startFrom = entry.Name
			w.stats.entries++
			if entry.IsDirectory {
				subDirs = append(subDirs, dir.Child(entry.Name))
			}
			return nil
		}, startFrom, false, walkPageSize)
		if err != nil {
			return err
		}
		for _, subDir := range subDirs {
			if err := w.walkDirectory(ctx, subDir, "", nil); err != nil {
				return err
			}
			w.advance(ctx, subDir)
		}
		if listed > 0 {
			w.advance(ctx, dir.Child(startFrom))
		}
		if listed < walkPageSize {
			return nil
		}
	}
}
