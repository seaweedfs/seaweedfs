package dailyrun

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStore is a minimal in-memory dispatcher.FilerStore for cursor tests.
type fakeStore struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{files: map[string][]byte{}}
}

func (s *fakeStore) key(dir, name string) string { return dir + "/" + name }

func (s *fakeStore) Read(_ context.Context, dir, name string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.files[s.key(dir, name)]
	if !ok {
		return nil, filer_pb.ErrNotFound
	}
	return append([]byte(nil), b...), nil
}

func (s *fakeStore) Save(_ context.Context, dir, name string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[s.key(dir, name)] = append([]byte(nil), content...)
	return nil
}

func TestFilerCursorPersister_EmptyLoadReturnsNotFound(t *testing.T) {
	p := &FilerCursorPersister{Store: newFakeStore()}
	c, found, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, Cursor{}, c)
}

func TestFilerCursorPersister_SaveLoadRoundTrip(t *testing.T) {
	p := &FilerCursorPersister{Store: newFakeStore()}
	in := Cursor{
		TsNs: 1700000000_000000123,
	}
	for i := range in.RuleSetHash {
		in.RuleSetHash[i] = byte(i + 1)
	}
	for i := range in.PromotedHash {
		in.PromotedHash[i] = byte(0xff - i)
	}
	require.NoError(t, p.Save(context.Background(), 3, in))

	out, found, err := p.Load(context.Background(), 3)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, in, out)
}

func TestFilerCursorPersister_IsolatesShards(t *testing.T) {
	p := &FilerCursorPersister{Store: newFakeStore()}
	a := Cursor{TsNs: 100}
	b := Cursor{TsNs: 200}
	require.NoError(t, p.Save(context.Background(), 0, a))
	require.NoError(t, p.Save(context.Background(), 1, b))

	got0, _, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	got1, _, err := p.Load(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, int64(100), got0.TsNs)
	assert.Equal(t, int64(200), got1.TsNs)
}

func TestFilerCursorPersister_CorruptDataReturnsError(t *testing.T) {
	store := newFakeStore()
	store.files[store.key(CursorDir, "shard-00.json")] = []byte("not json")
	p := &FilerCursorPersister{Store: store}
	_, _, err := p.Load(context.Background(), 0)
	require.Error(t, err)
}

func TestFilerCursorPersister_EmptyFileReturnsError(t *testing.T) {
	// A cursor file that exists but is empty signals a partial write
	// or external truncation. Treating it as "cold start" would silently
	// re-scan from time zero and burn through a meta-log window for no
	// reason. Pin that we error instead.
	store := newFakeStore()
	store.files[store.key(CursorDir, "shard-00.json")] = []byte{}
	p := &FilerCursorPersister{Store: store}
	_, _, err := p.Load(context.Background(), 0)
	require.Error(t, err)
}

func TestFilerCursorPersister_WrongVersionReturnsError(t *testing.T) {
	// Future schema bumps must not silently overwrite a cursor written
	// by a newer worker. Pin that contract here.
	store := newFakeStore()
	store.files[store.key(CursorDir, "shard-00.json")] = []byte(`{"version":42,"shard_id":0,"ts_ns":0,"rule_set_hash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","promoted_hash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`)
	p := &FilerCursorPersister{Store: store}
	_, _, err := p.Load(context.Background(), 0)
	require.Error(t, err)
}

func TestFilerCursorPersister_ShardIDMismatchReturnsError(t *testing.T) {
	// A file declaring shard=1 read by a shard=0 caller is corruption
	// (or a misroute); applying that cursor would advance the wrong
	// shard's state. Reject loudly.
	store := newFakeStore()
	// Build a valid v1 payload but with shard_id=1 in the body.
	in := Cursor{TsNs: 42}
	for i := range in.RuleSetHash {
		in.RuleSetHash[i] = byte(i + 1)
	}
	for i := range in.PromotedHash {
		in.PromotedHash[i] = byte(i)
	}
	p := &FilerCursorPersister{Store: store}
	require.NoError(t, p.Save(context.Background(), 1, in))
	// Reparent that file as if it lived at shard 0 — the test's fake
	// store only keys by name, so we copy the bytes manually.
	store.mu.Lock()
	store.files[store.key(CursorDir, "shard-00.json")] = store.files[store.key(CursorDir, "shard-01.json")]
	store.mu.Unlock()
	_, _, err := p.Load(context.Background(), 0)
	require.Error(t, err)
}

func TestFilerCursorPersister_HashLengthMismatchReturnsError(t *testing.T) {
	// Hand-rolled JSON with hashes shorter than 32 bytes. copy() would
	// silently zero-pad which makes the hash comparison match a value
	// that doesn't actually exist on disk. Reject loudly so an operator
	// fixes the persisted state.
	store := newFakeStore()
	store.files[store.key(CursorDir, "shard-00.json")] = []byte(`{"version":1,"shard_id":0,"ts_ns":0,"rule_set_hash":"AA==","promoted_hash":"AA=="}`)
	p := &FilerCursorPersister{Store: store}
	_, _, err := p.Load(context.Background(), 0)
	require.Error(t, err)
}

func TestFilerCursorPersister_NilStoreErrors(t *testing.T) {
	p := &FilerCursorPersister{}
	_, _, err := p.Load(context.Background(), 0)
	require.Error(t, err)
	require.Error(t, p.Save(context.Background(), 0, Cursor{}))
}
