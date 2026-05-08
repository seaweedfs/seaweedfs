package dispatcher

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// fakeFilerStore is an in-memory FilerStore for tests.
type fakeFilerStore struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newFakeFilerStore() *fakeFilerStore {
	return &fakeFilerStore{files: map[string][]byte{}}
}

func (f *fakeFilerStore) key(dir, name string) string { return dir + "/" + name }

func (f *fakeFilerStore) Read(ctx context.Context, dir, name string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.files[f.key(dir, name)]
	if !ok {
		return nil, filer_pb.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (f *fakeFilerStore) Save(ctx context.Context, dir, name string, content []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[f.key(dir, name)] = append([]byte(nil), content...)
	return nil
}

func mkKey(bucket string, kind s3lifecycle.ActionKind, hashByte byte) s3lifecycle.ActionKey {
	k := s3lifecycle.ActionKey{Bucket: bucket, ActionKind: kind}
	for i := range k.RuleHash {
		k.RuleHash[i] = hashByte
	}
	return k
}

func TestFilerPersisterEmptyLoadReturnsEmptyMap(t *testing.T) {
	p := &FilerPersister{Store: newFakeFilerStore()}
	state, err := p.Load(context.Background(), 0)
	if err != nil {
		t.Fatalf("Load on empty: %v", err)
	}
	if len(state) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(state))
	}
}

func TestFilerPersisterSaveLoadRoundTrip(t *testing.T) {
	p := &FilerPersister{Store: newFakeFilerStore()}
	ctx := context.Background()

	in := map[s3lifecycle.ActionKey]int64{
		mkKey("bucket-a", s3lifecycle.ActionKindExpirationDays, 0xAA):   12345,
		mkKey("bucket-b", s3lifecycle.ActionKindAbortMPU, 0xBB):         67890,
		mkKey("bucket-a", s3lifecycle.ActionKindNoncurrentDays, 0xCC):   54321,
	}
	if err := p.Save(ctx, 3, in); err != nil {
		t.Fatalf("Save: %v", err)
	}
	out, err := p.Load(ctx, 3)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("Load count=%d, want %d", len(out), len(in))
	}
	for k, v := range in {
		if got := out[k]; got != v {
			t.Fatalf("Load[%v]=%d, want %d", k, got, v)
		}
	}
}

func TestFilerPersisterIsolatesShards(t *testing.T) {
	p := &FilerPersister{Store: newFakeFilerStore()}
	ctx := context.Background()

	stateA := map[s3lifecycle.ActionKey]int64{mkKey("a", s3lifecycle.ActionKindExpirationDays, 1): 100}
	stateB := map[s3lifecycle.ActionKey]int64{mkKey("a", s3lifecycle.ActionKindExpirationDays, 1): 200}
	if err := p.Save(ctx, 0, stateA); err != nil {
		t.Fatalf("Save 0: %v", err)
	}
	if err := p.Save(ctx, 1, stateB); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	loadA, _ := p.Load(ctx, 0)
	loadB, _ := p.Load(ctx, 1)
	if loadA[mkKey("a", s3lifecycle.ActionKindExpirationDays, 1)] != 100 {
		t.Fatalf("shard 0 leaked from shard 1: %v", loadA)
	}
	if loadB[mkKey("a", s3lifecycle.ActionKindExpirationDays, 1)] != 200 {
		t.Fatalf("shard 1 reads stale: %v", loadB)
	}
}

func TestFilerPersisterSaveOverwrites(t *testing.T) {
	p := &FilerPersister{Store: newFakeFilerStore()}
	ctx := context.Background()
	k := mkKey("b", s3lifecycle.ActionKindExpirationDays, 0xAA)

	if err := p.Save(ctx, 0, map[s3lifecycle.ActionKey]int64{k: 100}); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	if err := p.Save(ctx, 0, map[s3lifecycle.ActionKey]int64{k: 200}); err != nil {
		t.Fatalf("Save 2: %v", err)
	}
	out, _ := p.Load(ctx, 0)
	if out[k] != 200 {
		t.Fatalf("overwrite not applied, got %d", out[k])
	}
}

func TestFilerPersisterSaveIsDeterministic(t *testing.T) {
	// Saving the same map twice must produce byte-identical content so the
	// on-disk file diffs cleanly when the state hasn't changed.
	store := newFakeFilerStore()
	p := &FilerPersister{Store: store}
	ctx := context.Background()

	in := map[s3lifecycle.ActionKey]int64{
		mkKey("zeta", s3lifecycle.ActionKindNoncurrentDays, 0xCC):  300,
		mkKey("alpha", s3lifecycle.ActionKindExpirationDays, 0xAA): 100,
		mkKey("alpha", s3lifecycle.ActionKindAbortMPU, 0xBB):       200,
	}
	if err := p.Save(ctx, 0, in); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	first := append([]byte(nil), store.files[store.key(CursorDir, cursorFileName(0))]...)

	if err := p.Save(ctx, 0, in); err != nil {
		t.Fatalf("Save 2: %v", err)
	}
	second := store.files[store.key(CursorDir, cursorFileName(0))]
	if string(first) != string(second) {
		t.Fatalf("non-deterministic save:\n first=%s\nsecond=%s", first, second)
	}
}

func TestFilerPersisterCorruptDataReturnsError(t *testing.T) {
	store := newFakeFilerStore()
	store.Save(context.Background(), CursorDir, "shard-00.json", []byte("not json"))

	p := &FilerPersister{Store: store}
	if _, err := p.Load(context.Background(), 0); err == nil {
		t.Fatal("expected decode error, got nil")
	}
}
