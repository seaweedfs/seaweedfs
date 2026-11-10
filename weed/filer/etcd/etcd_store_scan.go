package etcd

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

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
	for _, prefix := range prefixes {
		if err := scanPrefix(scanCtx, kv, keyPrefix, prefix, dirPath, rev, fn); err != nil {
			return err
		}
	}
	return nil
}

func scanPrefix(ctx context.Context, kv clientv3.KV, keyPrefix, prefix string, dirPath weed_util.FullPath, rev int64, fn func(ctx context.Context, entry *filer.Entry) error) error {
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
			if name == "" || len(name) == len(key) || isHardLinkKey(key) {
				continue
			}
			// The walk never reaches such a directory, and the stream cannot carry it.
			dir := key[:len(key)-len(name)-1]
			if !utf8.Valid(dir) {
				continue
			}
			entry := &filer.Entry{FullPath: weed_util.NewFullPath(string(dir), name)}
			if err := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(item.Value)); err != nil {
				return fmt.Errorf("scan decode %s: %v", entry.FullPath, err)
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

// isHardLinkKey recognizes a hard-link record stored beside the entries under
// its raw id (filer.NewHardLinkId): entry keys are valid UTF-8, random ids
// almost never are.
func isHardLinkKey(key []byte) bool {
	return len(key) == 17 && key[16] == filer.HARD_LINK_MARKER && !utf8.Valid(key)
}
