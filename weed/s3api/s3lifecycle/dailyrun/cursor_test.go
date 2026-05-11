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

func TestFilerCursorPersister_WrongVersionReturnsError(t *testing.T) {
	// Future schema bumps must not silently overwrite a cursor written
	// by a newer worker. Pin that contract here.
	store := newFakeStore()
	store.files[store.key(CursorDir, "shard-00.json")] = []byte(`{"version":42,"shard_id":0,"ts_ns":0,"rule_set_hash":null,"promoted_hash":null}`)
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
