package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

var _ filer.EntryScanner = (*EtcdStore)(nil)

var scanPageSize int64 = 1000

type scanRevisionKey struct{}

// atScanRevision pins a read made with a ScanEntries context to the scan's
// revision, so hard-link records resolve as of the same instant.
func atScanRevision(ctx context.Context, opts ...clientv3.OpOption) []clientv3.OpOption {
	if rev, ok := ctx.Value(scanRevisionKey{}).(int64); ok {
		opts = append(opts, clientv3.WithRev(rev))
	}
	return opts
}

// ScanEntries implements filer.EntryScanner. Every page is read at the
// revision of the first read, so a rename committed meanwhile shows up under
// exactly one of its names. If etcd compacts that revision before the scan
// ends, the scan fails rather than mixing revisions.
func (store *EtcdStore) ScanEntries(ctx context.Context, dirPath weed_util.FullPath, fn func(ctx context.Context, entry *filer.Entry) error) error {
	return scanEntries(ctx, store.client, store.etcdKeyPrefix, dirPath, fn)
}

func scanEntries(ctx context.Context, kv clientv3.KV, keyPrefix string, dirPath weed_util.FullPath, fn func(ctx context.Context, entry *filer.Entry) error) error {
	resp, err := kv.Get(ctx, keyPrefix+string(genKey(dirPath.DirAndName())))
	if err != nil {
		return fmt.Errorf("scan %s: %v", dirPath, err)
	}
	rev := resp.Header.Revision
	scanCtx := context.WithValue(ctx, scanRevisionKey{}, rev)
	if dirPath != "/" && len(resp.Kvs) > 0 {
		root := &filer.Entry{FullPath: dirPath}
		if err := root.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(resp.Kvs[0].Value)); err != nil {
			return fmt.Errorf("scan decode %s: %v", dirPath, err)
		}
		if err := fn(scanCtx, root); err != nil {
			return err
		}
	}

	// Children of dirPath, then everything deeper; "/" covers both.
	prefixes := []string{string(dirPath)}
	if dirPath != "/" {
		prefixes = []string{string(genDirectoryKeyPrefix(dirPath, "")), string(dirPath) + "/"}
	}
	sanitized := map[weed_util.FullPath]struct{}{}
	for _, prefix := range prefixes {
		if err := scanPrefix(scanCtx, kv, keyPrefix, prefix, dirPath, rev, sanitized, fn); err != nil {
			return err
		}
	}
	return nil
}

func scanPrefix(ctx context.Context, kv clientv3.KV, keyPrefix, prefix string, dirPath weed_util.FullPath, rev int64, sanitized map[weed_util.FullPath]struct{}, fn func(ctx context.Context, entry *filer.Entry) error) error {
	end := clientv3.GetPrefixRangeEnd(keyPrefix + prefix)
	for start := keyPrefix + prefix; ; {
		resp, err := kv.Get(ctx, start, clientv3.WithRange(end), clientv3.WithLimit(scanPageSize), clientv3.WithRev(rev))
		if errors.Is(err, rpctypes.ErrCompacted) {
			return fmt.Errorf("scan %s: etcd compacted revision %d before the scan finished; rerun it, or keep more revisions (etcd --auto-compaction-retention): %w", dirPath, rev, err)
		}
		if err != nil {
			return fmt.Errorf("scan %s: %w", dirPath, err)
		}
		for _, item := range resp.Kvs {
			key := item.Key[len(keyPrefix):]
			name := getNameFromKey(key)
			if name == "" || len(name) == len(key) {
				continue
			}
			// Names are sanitized when stored, directories are not; the stream
			// needs valid UTF-8, and fsck must still count the chunks.
			rawDir := string(key[:len(key)-len(name)-1])
			dir := weed_util.SanitizeUTF8Name(rawDir)
			entry := &filer.Entry{FullPath: weed_util.NewFullPath(dir, name)}
			if err := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(item.Value)); err != nil {
				return fmt.Errorf("scan decode %s: %v", entry.FullPath, err)
			}
			// A hard-link record is stored under its own id (filer.NewHardLinkId).
			if bytes.Equal(entry.HardLinkId, key) {
				continue
			}
			if dir != rawDir {
				if err := claimSanitizedPath(ctx, kv, keyPrefix, rev, entry.FullPath, weed_util.NewFullPath(rawDir, name), sanitized); err != nil {
					return err
				}
			}
			if err := fn(ctx, entry); err != nil {
				return err
			}
		}
		if !resp.More || len(resp.Kvs) == 0 {
			return nil
		}
		start = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}

// claimSanitizedPath fails the scan when the path an entry is reported under
// names a different entry, so acting on it (fsck deleting it) cannot reach
// another file.
func claimSanitizedPath(ctx context.Context, kv clientv3.KV, keyPrefix string, rev int64, p, rawPath weed_util.FullPath, sanitized map[weed_util.FullPath]struct{}) error {
	_, taken := sanitized[p]
	if !taken {
		resp, err := kv.Get(ctx, keyPrefix+string(genKey(p.DirAndName())), clientv3.WithRev(rev), clientv3.WithKeysOnly())
		if err != nil {
			return fmt.Errorf("scan lookup %s: %w", p, err)
		}
		taken = len(resp.Kvs) > 0
	}
	if taken {
		return fmt.Errorf("scan: entry %q has a directory that is not valid UTF-8 and would be reported as %s, which names another entry; rename or delete one of them", string(rawPath), p)
	}
	sanitized[p] = struct{}{}
	return nil
}
