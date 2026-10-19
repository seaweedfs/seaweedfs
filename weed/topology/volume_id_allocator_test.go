package topology

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// fakeEtcdKV is an in-memory clientv3.KV honoring CreateRevision/ModRevision
// compare-and-swap, enough to exercise the allocator's CAS loop.
type fakeEtcdKV struct {
	mu   sync.Mutex
	rev  int64
	data map[string]*mvccpb.KeyValue
	// beforeCmp runs inside Commit before the compares are evaluated, to
	// model a competing writer landing between the read and the CAS.
	beforeCmp func()
	getErr    error
	commitErr error
}

func newFakeEtcdKV() *fakeEtcdKV {
	return &fakeEtcdKV{data: make(map[string]*mvccpb.KeyValue)}
}

func (kv *fakeEtcdKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.getErr != nil {
		return nil, kv.getErr
	}
	if v, ok := kv.data[key]; ok {
		cp := *v
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{&cp}}, nil
	}
	return &clientv3.GetResponse{}, nil
}

func (kv *fakeEtcdKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.applyPut(key, val)
	return &clientv3.PutResponse{}, nil
}

func (kv *fakeEtcdKV) applyPut(key, val string) {
	kv.rev++
	if v, ok := kv.data[key]; ok {
		v.Value = []byte(val)
		v.ModRevision = kv.rev
	} else {
		kv.data[key] = &mvccpb.KeyValue{Key: []byte(key), Value: []byte(val), CreateRevision: kv.rev, ModRevision: kv.rev}
	}
}

func (kv *fakeEtcdKV) GetStream(ctx context.Context, key string, opts ...clientv3.OpOption) (clientv3.GetStreamChan, error) {
	return nil, nil
}

func (kv *fakeEtcdKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.data, key)
	return &clientv3.DeleteResponse{}, nil
}

func (kv *fakeEtcdKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (kv *fakeEtcdKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (kv *fakeEtcdKV) Txn(ctx context.Context) clientv3.Txn {
	return &fakeEtcdTxn{kv: kv}
}

type fakeEtcdTxn struct {
	kv   *fakeEtcdKV
	cmps []clientv3.Cmp
	ops  []clientv3.Op
}

func (t *fakeEtcdTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	t.cmps = append(t.cmps, cs...)
	return t
}

func (t *fakeEtcdTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

func (t *fakeEtcdTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	return t
}

func (t *fakeEtcdTxn) Commit() (*clientv3.TxnResponse, error) {
	t.kv.mu.Lock()
	defer t.kv.mu.Unlock()
	if t.kv.beforeCmp != nil {
		t.kv.beforeCmp()
	}
	for _, cmp := range t.cmps {
		if !t.kv.evalCmp(cmp) {
			return &clientv3.TxnResponse{Succeeded: false}, nil
		}
	}
	for _, op := range t.ops {
		if op.IsPut() {
			t.kv.applyPut(string(op.KeyBytes()), string(op.ValueBytes()))
		}
	}
	if t.kv.commitErr != nil {
		return nil, t.kv.commitErr
	}
	return &clientv3.TxnResponse{Succeeded: true}, nil
}

func (kv *fakeEtcdKV) evalCmp(cmp clientv3.Cmp) bool {
	c := cmp.GetCompare()
	if c.Result != etcdserverpb.Compare_EQUAL {
		return false
	}
	existing := kv.data[string(c.Key)]
	switch u := c.TargetUnion.(type) {
	case *etcdserverpb.Compare_CreateRevision:
		var rev int64
		if existing != nil {
			rev = existing.CreateRevision
		}
		return rev == u.CreateRevision
	case *etcdserverpb.Compare_ModRevision:
		var rev int64
		if existing != nil {
			rev = existing.ModRevision
		}
		return rev == u.ModRevision
	}
	return false
}

func TestEtcdAllocatorSeedsFromLocalBound(t *testing.T) {
	alloc := newEtcdVolumeIdAllocator(newFakeEtcdKV(), "/sw/", time.Second, func() needle.VolumeId {
		return 41
	})
	for i, want := range []needle.VolumeId{42, 43, 44} {
		got, err := alloc.NextVolumeId()
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("alloc %d = %d, want %d", i, got, want)
		}
	}
}

func TestEtcdAllocatorNeverBelowLocalBound(t *testing.T) {
	kv := newFakeEtcdKV()
	kv.Put(context.Background(), "seaweedfs/max_volume_id", "10")
	alloc := newEtcdVolumeIdAllocator(kv, "seaweedfs", time.Second, func() needle.VolumeId {
		return 41
	})
	got, err := alloc.NextVolumeId()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 42 {
		t.Fatalf("a stale stored counter must not reissue ids: got %d", got)
	}
}

func TestEtcdAllocatorResumesFromStoredKey(t *testing.T) {
	kv := newFakeEtcdKV()
	kv.Put(context.Background(), "seaweedfs/max_volume_id", "100")
	alloc := newEtcdVolumeIdAllocator(kv, "seaweedfs", time.Second, func() needle.VolumeId {
		return 41
	})
	got, err := alloc.NextVolumeId()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 101 {
		t.Fatalf("stored counter wins over the seed: got %d", got)
	}
}

func TestEtcdAllocatorRetriesOnCasConflict(t *testing.T) {
	kv := newFakeEtcdKV()
	// a competing writer lands the key once, between the read and the CAS
	kv.beforeCmp = func() {
		kv.applyPut("seaweedfs/max_volume_id", "55")
		kv.beforeCmp = nil
	}
	alloc := newEtcdVolumeIdAllocator(kv, "seaweedfs", time.Second, func() needle.VolumeId {
		return 5
	})
	got, err := alloc.NextVolumeId()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 56 {
		t.Fatalf("CAS retry should allocate 56, got %d", got)
	}
}

func TestEtcdAllocatorSkipsIdBelowFreshBound(t *testing.T) {
	kv := newFakeEtcdKV()
	var bound atomic.Uint32
	bound.Store(5)
	// a heartbeat raises the local bound while the CAS is in flight; the
	// committed 6 must not be handed out
	kv.beforeCmp = func() {
		bound.Store(43)
		kv.beforeCmp = nil
	}
	alloc := newEtcdVolumeIdAllocator(kv, "seaweedfs", time.Second, func() needle.VolumeId {
		return needle.VolumeId(bound.Load())
	})
	got, err := alloc.NextVolumeId()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 44 {
		t.Fatalf("an id at or below the fresher bound must be skipped, got %d", got)
	}
}

func TestEtcdAllocatorReadError(t *testing.T) {
	kv := newFakeEtcdKV()
	kv.getErr = errors.New("etcd read failed")
	alloc := newEtcdVolumeIdAllocator(kv, "seaweedfs", time.Second, func() needle.VolumeId {
		return 41
	})
	if _, err := alloc.NextVolumeId(); err == nil {
		t.Fatal("a failed read must return an error, not an id")
	}
	kv.getErr = nil
	got, err := alloc.NextVolumeId()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestEtcdAllocatorCommitError(t *testing.T) {
	kv := newFakeEtcdKV()
	kv.commitErr = errors.New("etcd commit failed")
	alloc := newEtcdVolumeIdAllocator(kv, "seaweedfs", time.Second, func() needle.VolumeId {
		return 41
	})
	if _, err := alloc.NextVolumeId(); err == nil {
		t.Fatal("a failed commit must return an error, not an id")
	}
	kv.commitErr = nil
	got, err := alloc.NextVolumeId()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if got != 43 {
		t.Fatalf("the id written by the failed commit must be skipped, got %d", got)
	}
}

func TestEtcdAllocatorExhaustion(t *testing.T) {
	kv := newFakeEtcdKV()
	kv.Put(context.Background(), "seaweedfs/max_volume_id", strconv.FormatUint(math.MaxUint32, 10))
	alloc := newEtcdVolumeIdAllocator(kv, "seaweedfs", time.Second, func() needle.VolumeId {
		return 5
	})
	if _, err := alloc.NextVolumeId(); err == nil {
		t.Fatal("expected exhaustion error at MaxUint32")
	}
}
