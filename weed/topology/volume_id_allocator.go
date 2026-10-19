package topology

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeIdAllocator hands out the next volume id. The builtin path advances
// the raft-applied high-water mark; an external implementation makes the
// boundary survive total master state loss.
type VolumeIdAllocator interface {
	NextVolumeId() (needle.VolumeId, error)
}

// EtcdVolumeIdAllocator keeps the volume id counter in a single etcd key,
// advanced by compare-and-swap. Unlike the raft/mirror/echo layers it has no
// residual window: an id cannot be issued without the etcd write committing.
type EtcdVolumeIdAllocator struct {
	kv      clientv3.KV
	key     string
	timeout time.Duration
	seed    func() needle.VolumeId
}

func NewEtcdVolumeIdAllocator(kv clientv3.KV, keyPrefix string, timeout time.Duration, seed func() needle.VolumeId) *EtcdVolumeIdAllocator {
	return &EtcdVolumeIdAllocator{
		kv:      kv,
		key:     strings.TrimRight(keyPrefix, "/") + "/max_volume_id",
		timeout: timeout,
		seed:    seed,
	}
}

func (a *EtcdVolumeIdAllocator) NextVolumeId() (needle.VolumeId, error) {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	for {
		resp, err := a.kv.Get(ctx, a.key)
		if err != nil {
			return 0, fmt.Errorf("volume id allocator read: %w", err)
		}
		var cur uint64
		cmp := clientv3.Compare(clientv3.CreateRevision(a.key), "=", 0)
		if len(resp.Kvs) > 0 {
			if cur, err = strconv.ParseUint(string(resp.Kvs[0].Value), 10, 32); err != nil {
				return 0, fmt.Errorf("volume id counter %q: %w", resp.Kvs[0].Value, err)
			}
			cmp = clientv3.Compare(clientv3.ModRevision(a.key), "=", resp.Kvs[0].ModRevision)
		} else {
			cur = uint64(a.seed())
		}
		if cur >= math.MaxUint32 {
			return 0, fmt.Errorf("volume id counter exhausted at %d", cur)
		}
		committed, err := a.kv.Txn(ctx).If(cmp).Then(clientv3.OpPut(a.key, strconv.FormatUint(cur+1, 10))).Commit()
		if err != nil {
			return 0, fmt.Errorf("volume id allocator commit: %w", err)
		}
		if committed.Succeeded {
			return needle.VolumeId(cur + 1), nil
		}
	}
}
