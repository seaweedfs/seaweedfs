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
	client  *clientv3.Client
	key     string
	timeout time.Duration
	seed    func() needle.VolumeId
}

func NewEtcdVolumeIdAllocator(client *clientv3.Client, keyPrefix string, timeout time.Duration, seed func() needle.VolumeId) *EtcdVolumeIdAllocator {
	a := newEtcdVolumeIdAllocator(client.KV, keyPrefix, timeout, seed)
	a.client = client
	return a
}

func newEtcdVolumeIdAllocator(kv clientv3.KV, keyPrefix string, timeout time.Duration, seed func() needle.VolumeId) *EtcdVolumeIdAllocator {
	return &EtcdVolumeIdAllocator{
		kv:      kv,
		key:     strings.TrimRight(keyPrefix, "/") + "/max_volume_id",
		timeout: timeout,
		seed:    seed,
	}
}

func (a *EtcdVolumeIdAllocator) Close() error {
	if a.client == nil {
		return nil
	}
	return a.client.Close()
}

func (a *EtcdVolumeIdAllocator) NextVolumeId() (needle.VolumeId, error) {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	var seedBound uint64
	seeded := false
	for {
		resp, err := a.kv.Get(ctx, a.key)
		if err != nil {
			return 0, fmt.Errorf("volume id allocator read: %w", err)
		}
		if !seeded {
			// A stale etcd counter (restored backup, re-enabled allocator) must
			// never go below the bound this master already knows.
			seedBound, seeded = uint64(a.seed()), true
		}
		var cur uint64
		cmp := clientv3.Compare(clientv3.CreateRevision(a.key), "=", 0)
		if len(resp.Kvs) > 0 {
			if cur, err = strconv.ParseUint(string(resp.Kvs[0].Value), 10, 32); err != nil {
				return 0, fmt.Errorf("volume id counter %q: %w", resp.Kvs[0].Value, err)
			}
			cmp = clientv3.Compare(clientv3.ModRevision(a.key), "=", resp.Kvs[0].ModRevision)
		}
		if seedBound > cur {
			cur = seedBound
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
